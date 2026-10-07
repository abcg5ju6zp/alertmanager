// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package silence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// journalFileMagic prefixes every silence journal file.
var journalFileMagic = [8]byte{'A', 'M', 'S', 'I', 'L', 'J', 'N', 'L'}

// journalFileSuffix is appended to the snapshot file path to derive the
// journal file path.
const journalFileSuffix = ".journal"

const (
	journalFileVersion byte = 1

	journalRecordBaseline  byte = 1
	journalRecordPending   byte = 2
	journalRecordConfirmed byte = 3

	journalOpUpsert byte = 1
	journalOpGC     byte = 2
)

// journalMaxPayloadSize bounds a single record payload. A silence snapshot
// batch is far smaller than this; a larger length can only come from a
// corrupt frame header and must not trigger a huge allocation.
const journalMaxPayloadSize = 256 << 20 // 256 MiB

// journalFrameHeaderSize is one record type byte, eight bytes of sequence
// number and four bytes of payload length. A trailing four byte CRC32
// checksum follows the payload.
const journalFrameHeaderSize = 1 + 8 + 4

// journalOp is a recoverable candidate operation. Exactly one of silence or
// ids is populated depending on the operation kind.
type journalOp struct {
	seq     uint64
	gc      bool
	silence *pb.MeshSilence // Populated for upsert operations.
	ids     []string        // Populated for GC operations.
}

// journal is an append-only log of candidate silence operations. PENDING
// records hold recoverable operations; CONFIRMED records mark operations
// whose snapshot has been durably published; BASELINE records mark that all
// sequences up to (and including) the baseline sequence are contained in the
// snapshot file and therefore do not need to be replayed.
type journal struct {
	path string
	mtx  sync.Mutex
	f    *os.File

	nextSeq uint64
	baseSeq uint64
	pending map[uint64]journalOp

	// truncated reports whether a torn or corrupt tail was discarded while
	// opening the journal.
	truncated bool
}

// openJournal opens the journal at path, creating it if it does not exist.
// A torn or corrupt tail is discarded and the file is truncated back to the
// last intact frame. A corrupt header or an unsupported version is reported
// as an error.
func openJournal(path string) (*journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open silence journal %q: %w", path, err)
	}

	j := &journal{
		path:    path,
		f:       f,
		pending: map[uint64]journalOp{},
		// Sequences start at 1 so that baseline 0 unambiguously means that
		// nothing has been confirmed yet.
		nextSeq: 1,
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat silence journal %q: %w", path, err)
	}

	if fi.Size() == 0 {
		if err := writeJournalHeader(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("sync silence journal %q: %w", path, err)
		}
		syncDirBestEffort(filepath.Dir(path))
		return j, nil
	}

	if fi.Size() < int64(len(journalFileMagic)+1) {
		// The header itself was torn; restart from an empty journal. The
		// confirmed snapshot remains the source of truth.
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("truncate torn silence journal %q: %w", path, err)
		}
		if err := writeJournalHeader(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("sync silence journal %q: %w", path, err)
		}
		j.truncated = true
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			_ = f.Close()
			return nil, err
		}
		return j, nil
	}

	base, pending, nextSeq, validOffset, torn, err := parseJournal(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if torn {
		if err := f.Truncate(validOffset); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("truncate corrupt silence journal %q: %w", path, err)
		}
		j.truncated = true
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}

	j.baseSeq = base
	j.pending = pending
	j.nextSeq = nextSeq
	return j, nil
}

// baseline returns the sequence through which all operations are contained in
// the confirmed snapshot.
func (j *journal) baseline() uint64 {
	j.mtx.Lock()
	defer j.mtx.Unlock()
	return j.baseSeq
}

// nextSequence returns the sequence that the next PENDING record will use.
func (j *journal) nextSequence() uint64 {
	j.mtx.Lock()
	defer j.mtx.Unlock()
	return j.nextSeq
}

// hadTornTail reports whether opening the journal discarded a corrupt tail.
func (j *journal) hadTornTail() bool {
	j.mtx.Lock()
	defer j.mtx.Unlock()
	return j.truncated
}

// appendUpsert appends a PENDING record carrying an upsert of the given
// silence and returns its sequence number.
func (j *journal) appendUpsert(msil *pb.MeshSilence) (uint64, error) {
	payload, err := encodeUpsertPayload(msil)
	if err != nil {
		return 0, err
	}
	op := journalOp{gc: false, silence: cloneMeshSilence(msil)}
	return j.appendPending(payload, op)
}

// appendGC appends a PENDING record carrying the garbage collection of the
// given silence IDs and returns its sequence number.
func (j *journal) appendGC(ids []string) (uint64, error) {
	payload, err := encodeGCPayload(ids)
	if err != nil {
		return 0, err
	}
	cp := make([]string, len(ids))
	copy(cp, ids)
	op := journalOp{gc: true, ids: cp}
	return j.appendPending(payload, op)
}

func (j *journal) appendPending(payload []byte, op journalOp) (uint64, error) {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	seq := j.nextSeq
	if _, err := j.f.Write(encodeJournalFrame(journalRecordPending, seq, payload)); err != nil {
		return 0, fmt.Errorf("write journal pending record: %w", err)
	}
	if err := j.f.Sync(); err != nil {
		return 0, fmt.Errorf("sync journal pending record: %w", err)
	}
	op.seq = seq
	j.pending[seq] = op
	j.nextSeq = seq + 1
	return seq, nil
}

// appendConfirmed marks the given PENDING sequence as durably confirmed.
func (j *journal) appendConfirmed(seq uint64) error {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	if _, err := j.f.Write(encodeJournalFrame(journalRecordConfirmed, seq, nil)); err != nil {
		return fmt.Errorf("write journal confirmed record: %w", err)
	}
	if err := j.f.Sync(); err != nil {
		return fmt.Errorf("sync journal confirmed record: %w", err)
	}
	delete(j.pending, seq)
	return nil
}

// unconfirmed returns PENDING operations without a matching CONFIRMED record,
// ordered by sequence. Operations at or below the baseline are not returned.
func (j *journal) unconfirmed() []journalOp {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	sortSeqs := make([]uint64, 0, len(j.pending))
	for seq := range j.pending {
		if seq > j.baseSeq {
			sortSeqs = append(sortSeqs, seq)
		}
	}
	slices.Sort(sortSeqs)

	ops := make([]journalOp, 0, len(sortSeqs))
	for _, seq := range sortSeqs {
		op := j.pending[seq]
		// Return copies so recovery can mutate the silence (for example
		// upgrading deprecated fields) without corrupting journal state.
		if op.silence != nil {
			op.silence = cloneMeshSilence(op.silence)
		}
		if op.ids != nil {
			op.ids = append([]string(nil), op.ids...)
		}
		ops = append(ops, op)
	}
	return ops
}

// checkpointConfirmed advances the baseline to the highest sequence that has
// no unconfirmed operation at or below it. PENDING operations above the new
// baseline are retained verbatim. It is safe to call when nothing can be
// compacted; in that case the journal is left untouched.
func (j *journal) checkpointConfirmed() error {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	seq := j.nextSeq - 1
	for pendingSeq := range j.pending {
		if pendingSeq <= seq {
			seq = pendingSeq - 1
		}
	}
	if seq <= j.baseSeq {
		return nil
	}
	return j.checkpointLocked(seq)
}

// checkpoint rewrites the journal so that it starts with a BASELINE at seq
// and retains only PENDING operations with a higher sequence. The rewrite is
// durable (temporary file, fsync, atomic rename and directory fsync) before
// this method returns.
func (j *journal) checkpoint(seq uint64) error {
	j.mtx.Lock()
	defer j.mtx.Unlock()
	return j.checkpointLocked(seq)
}

// checkpointLocked implements checkpoint. The caller must hold j.mtx.
func (j *journal) checkpointLocked(seq uint64) error {
	if seq < j.baseSeq {
		return fmt.Errorf("journal checkpoint sequence %d is older than baseline %d", seq, j.baseSeq)
	}
	for pendingSeq := range j.pending {
		if pendingSeq <= seq {
			return fmt.Errorf("journal checkpoint sequence %d would discard unconfirmed operation %d", seq, pendingSeq)
		}
	}

	var buf bytes.Buffer
	if err := writeJournalHeader(&buf); err != nil {
		return err
	}
	if _, err := buf.Write(encodeJournalFrame(journalRecordBaseline, seq, nil)); err != nil {
		return err
	}

	pendingSeqs := make([]uint64, 0, len(j.pending))
	for pendingSeq := range j.pending {
		pendingSeqs = append(pendingSeqs, pendingSeq)
	}
	slices.Sort(pendingSeqs)
	for _, pendingSeq := range pendingSeqs {
		op := j.pending[pendingSeq]
		payload, err := op.payload()
		if err != nil {
			return err
		}
		if _, err := buf.Write(encodeJournalFrame(journalRecordPending, pendingSeq, payload)); err != nil {
			return err
		}
	}

	dir := filepath.Dir(j.path)
	tmp, err := os.CreateTemp(dir, ".silences-journal-*")
	if err != nil {
		return fmt.Errorf("create journal checkpoint temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write journal checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync journal checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close journal checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, j.path); err != nil {
		return fmt.Errorf("replace journal during checkpoint: %w", err)
	}
	syncDirBestEffort(dir)

	// The old handle now points at the unlinked inode; reopen the new file.
	if err := j.f.Close(); err != nil {
		return fmt.Errorf("close replaced journal: %w", err)
	}
	f, err := os.OpenFile(j.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen journal after checkpoint: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	j.f = f
	j.baseSeq = seq
	return nil
}

// close releases the journal's file handle.
func (j *journal) close() error {
	j.mtx.Lock()
	defer j.mtx.Unlock()
	if j.f == nil {
		return nil
	}
	err := j.f.Close()
	j.f = nil
	return err
}

// payload re-encodes the operation's PENDING payload.
func (op journalOp) payload() ([]byte, error) {
	if op.gc {
		return encodeGCPayload(op.ids)
	}
	return encodeUpsertPayload(op.silence)
}

func cloneMeshSilence(msil *pb.MeshSilence) *pb.MeshSilence {
	return proto.Clone(msil).(*pb.MeshSilence)
}

func encodeUpsertPayload(msil *pb.MeshSilence) ([]byte, error) {
	body, err := proto.Marshal(msil)
	if err != nil {
		return nil, fmt.Errorf("marshal journal upsert: %w", err)
	}
	payload := make([]byte, 1+len(body))
	payload[0] = journalOpUpsert
	copy(payload[1:], body)
	return payload, nil
}

func encodeGCPayload(ids []string) ([]byte, error) {
	var buf bytes.Buffer
	head := make([]byte, 4)
	binary.BigEndian.PutUint32(head, uint32(len(ids)))
	if _, err := buf.Write(head); err != nil {
		return nil, err
	}
	lenBuf := make([]byte, 2)
	for _, id := range ids {
		if len(id) > 0xffff {
			return nil, fmt.Errorf("silence ID %q is too long for a journal GC record", id)
		}
		binary.BigEndian.PutUint16(lenBuf, uint16(len(id)))
		if _, err := buf.Write(lenBuf); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(id); err != nil {
			return nil, err
		}
	}
	payload := make([]byte, 1+buf.Len())
	payload[0] = journalOpGC
	copy(payload[1:], buf.Bytes())
	return payload, nil
}

func decodeJournalOpPayload(payload []byte) (journalOp, error) {
	if len(payload) < 1 {
		return journalOp{}, errors.New("empty journal operation payload")
	}
	switch payload[0] {
	case journalOpUpsert:
		var msil pb.MeshSilence
		if err := proto.Unmarshal(payload[1:], &msil); err != nil {
			return journalOp{}, fmt.Errorf("unmarshal journal upsert: %w", err)
		}
		if msil.Silence == nil {
			return journalOp{}, ErrInvalidState
		}
		return journalOp{gc: false, silence: &msil}, nil
	case journalOpGC:
		if len(payload) < 5 {
			return journalOp{}, errors.New("journal GC payload too short")
		}
		r := bytes.NewReader(payload[1:])
		var head [4]byte
		if _, err := io.ReadFull(r, head[:]); err != nil {
			return journalOp{}, fmt.Errorf("read journal GC count: %w", err)
		}
		n := binary.BigEndian.Uint32(head[:])
		// Every entry needs at least its two byte length prefix, so a larger
		// count can only come from a corrupt payload and must not be turned
		// into a huge allocation.
		if uint64(n) > uint64(len(payload)-5)/2 {
			return journalOp{}, fmt.Errorf("journal GC count %d exceeds the payload capacity", n)
		}
		ids := make([]string, 0, n)
		lenBuf := make([]byte, 2)
		for range n {
			if _, err := io.ReadFull(r, lenBuf); err != nil {
				return journalOp{}, fmt.Errorf("read journal GC ID length: %w", err)
			}
			idLen := binary.BigEndian.Uint16(lenBuf)
			idBuf := make([]byte, idLen)
			if _, err := io.ReadFull(r, idBuf); err != nil {
				return journalOp{}, fmt.Errorf("read journal GC ID: %w", err)
			}
			ids = append(ids, string(idBuf))
		}
		return journalOp{gc: true, ids: ids}, nil
	default:
		return journalOp{}, fmt.Errorf("unknown journal operation kind %d", payload[0])
	}
}

// encodeJournalFrame frames a record with its checksum. The checksum covers
// the record type, sequence, payload length and payload.
func encodeJournalFrame(recordType byte, seq uint64, payload []byte) []byte {
	frame := make([]byte, journalFrameHeaderSize+len(payload)+4)
	frame[0] = recordType
	binary.BigEndian.PutUint64(frame[1:9], seq)
	binary.BigEndian.PutUint32(frame[9:13], uint32(len(payload)))
	copy(frame[13:13+len(payload)], payload)
	crc := crc32.ChecksumIEEE(frame[:journalFrameHeaderSize+len(payload)])
	binary.BigEndian.PutUint32(frame[journalFrameHeaderSize+len(payload):], crc)
	return frame
}

// parseJournal scans the journal after its file header. It returns the
// effective baseline, the still-unconfirmed PENDING operations, the next
// usable sequence, the byte offset of the last intact frame boundary, and
// whether a torn or corrupt tail was encountered.
func parseJournal(r io.ReaderAt) (base uint64, pending map[uint64]journalOp, nextSeq uint64, validOffset int64, torn bool, err error) {
	var header [len(journalFileMagic) + 1]byte
	if _, err := r.ReadAt(header[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return 0, nil, 0, 0, false, fmt.Errorf("read silence journal header: %w", err)
	}
	if !bytes.Equal(header[:len(journalFileMagic)], journalFileMagic[:]) {
		return 0, nil, 0, 0, false, errors.New("silence journal has an unknown file magic")
	}
	if header[len(journalFileMagic)] != journalFileVersion {
		return 0, nil, 0, 0, false, fmt.Errorf("unsupported silence journal version %d", header[len(journalFileMagic)])
	}

	base = 0
	pending = map[uint64]journalOp{}
	var maxSeq uint64
	offset := int64(len(header))

	br := io.NewSectionReader(r, offset, 1<<63-1)
	frameHead := make([]byte, journalFrameHeaderSize)
	crcBuf := make([]byte, 4)

	var frameStart int64
	for {
		frameStart = offset
		n, err := io.ReadFull(br, frameHead)
		if errors.Is(err, io.EOF) {
			break // Clean end of file.
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			torn = true
			break
		}
		if err != nil {
			return 0, nil, 0, 0, false, fmt.Errorf("read silence journal frame header: %w", err)
		}
		offset += int64(n)

		recordType := frameHead[0]
		seq := binary.BigEndian.Uint64(frameHead[1:9])
		payloadLen := binary.BigEndian.Uint32(frameHead[9:13])
		maxSeq = max(maxSeq, seq)

		if payloadLen > journalMaxPayloadSize {
			// Implausible length: the header is corrupt and the tail must
			// be discarded without allocating the claimed payload.
			torn = true
			break
		}

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			pn, perr := io.ReadFull(br, payload)
			if perr != nil {
				if errors.Is(perr, io.ErrUnexpectedEOF) {
					torn = true
					break
				}
				return 0, nil, 0, 0, false, fmt.Errorf("read silence journal frame payload: %w", perr)
			}
			offset += int64(pn)
		}

		cn, cerr := io.ReadFull(br, crcBuf)
		if cerr != nil {
			if errors.Is(cerr, io.EOF) || errors.Is(cerr, io.ErrUnexpectedEOF) {
				torn = true
				break
			}
			return 0, nil, 0, 0, false, fmt.Errorf("read silence journal frame checksum: %w", cerr)
		}
		offset += int64(cn)

		wantCRC := crc32.ChecksumIEEE(frameHead)
		wantCRC = crc32.Update(wantCRC, crc32.IEEETable, payload)
		if binary.BigEndian.Uint32(crcBuf) != wantCRC {
			torn = true
			break
		}

		switch recordType {
		case journalRecordBaseline:
			if payloadLen != 0 {
				// Malformed structural record: treat the tail as corrupt.
				torn = true
			} else {
				base = max(base, seq)
				for pendingSeq := range pending {
					if pendingSeq <= base {
						delete(pending, pendingSeq)
					}
				}
			}
		case journalRecordPending:
			op, perr := decodeJournalOpPayload(payload)
			if perr != nil {
				torn = true
			} else if seq > base {
				op.seq = seq
				pending[seq] = op
			}
		case journalRecordConfirmed:
			delete(pending, seq)
		default:
			torn = true
		}
		if torn {
			break
		}
	}

	if torn {
		// Discard the whole damaged frame and keep everything before it.
		return base, pending, maxSeq + 1, frameStart, true, nil
	}
	return base, pending, maxSeq + 1, offset, false, nil
}

func writeJournalHeader(w io.Writer) error {
	if _, err := w.Write(journalFileMagic[:]); err != nil {
		return fmt.Errorf("write silence journal magic: %w", err)
	}
	if _, err := w.Write([]byte{journalFileVersion}); err != nil {
		return fmt.Errorf("write silence journal version: %w", err)
	}
	return nil
}

// syncDirBestEffort fsyncs a directory so that a recently renamed file is
// durable. Errors are ignored because not all filesystems permit fsync on
// directory descriptors.
func syncDirBestEffort(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}
