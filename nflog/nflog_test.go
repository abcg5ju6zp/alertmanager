// Copyright 2016 Prometheus Team
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

package nflog

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/prometheus/alertmanager/nflog/nflogpb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLogGC(t *testing.T) {
	now := time.Now()
	// We only care about key names and expiration timestamps.
	newEntry := func(ts time.Time) *pb.MeshEntry {
		return &pb.MeshEntry{
			ExpiresAt: timestamppb.New(ts),
		}
	}

	l := &Log{
		st: state{
			"a1": newEntry(now),
			"a2": newEntry(now.Add(time.Second)),
			"a3": newEntry(now.Add(-time.Second)),
		},
		metrics: newMetrics(prometheus.NewRegistry()),
	}
	n, err := l.GC()
	require.NoError(t, err, "unexpected error in garbage collection")
	require.Equal(t, 2, n, "unexpected number of removed entries")

	expected := state{
		"a2": newEntry(now.Add(time.Second)),
	}
	require.Equal(t, expected, l.st, "unexpected state after garbage collection")
}

func TestLogSnapshot(t *testing.T) {
	// Check whether storing and loading the snapshot is symmetric.
	now := time.Now().UTC()

	cases := []struct {
		entries []*pb.MeshEntry
	}{
		{
			entries: []*pb.MeshEntry{
				{
					Entry: &pb.Entry{
						GroupKey:  []byte("d8e8fca2dc0f896fd7cb4cb0031ba249"),
						Receiver:  &pb.Receiver{GroupName: "abc", Integration: "test1", Idx: 1},
						GroupHash: []byte("126a8a51b9d1bbd07fddc65819a542c3"),
						Resolved:  false,
						Timestamp: timestamppb.New(now),
					},
					ExpiresAt: timestamppb.New(now),
				}, {
					Entry: &pb.Entry{
						GroupKey:  []byte("d8e8fca2dc0f8abce7cb4cb0031ba249"),
						Receiver:  &pb.Receiver{GroupName: "def", Integration: "test2", Idx: 29},
						GroupHash: []byte("122c2331b9d1bbd07fddc65819a542c3"),
						Resolved:  true,
						Timestamp: timestamppb.New(now),
					},
					ExpiresAt: timestamppb.New(now),
				}, {
					Entry: &pb.Entry{
						GroupKey:  []byte("aaaaaca2dc0f896fd7cb4cb0031ba249"),
						Receiver:  &pb.Receiver{GroupName: "ghi", Integration: "test3", Idx: 0},
						GroupHash: []byte("126a8a51b9d1bbd07fddc6e3e3e542c3"),
						Resolved:  false,
						Timestamp: timestamppb.New(now),
					},
					ExpiresAt: timestamppb.New(now),
				},
			},
		},
	}

	for _, c := range cases {
		f, err := os.CreateTemp(t.TempDir(), "snapshot")
		require.NoError(t, err, "creating temp file failed")

		l1 := &Log{
			st:      state{},
			metrics: newMetrics(nil),
		}
		// Setup internal state manually.
		for _, e := range c.entries {
			l1.st[stateKey(string(e.Entry.GroupKey), e.Entry.Receiver)] = e
		}
		_, err = l1.Snapshot(f)
		require.NoError(t, err, "creating snapshot failed")
		require.NoError(t, f.Close(), "closing snapshot file failed")

		f, err = os.Open(f.Name())
		require.NoError(t, err, "opening snapshot file failed")

		// Check again against new nlog instance.
		l2 := &Log{}
		err = l2.loadSnapshot(f)
		require.NoError(t, err, "error loading snapshot")

		for id, expected := range l1.st {
			actual, ok := l2.st[id]
			require.True(t, ok, "silence %s missing from decoded state", id)
			require.True(t, proto.Equal(expected, actual), "silence %s mismatch after decoding", id)
		}

		require.NoError(t, f.Close(), "closing snapshot file failed")
	}
}

func TestWithMaintenance_SupportsCustomCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "snapshot")
		require.NoError(t, err, "creating temp file failed")
		stopc := make(chan struct{})
		reg := prometheus.NewPedanticRegistry()
		opts := Options{
			Metrics:      reg,
			SnapshotFile: f.Name(),
		}

		l, err := New(opts)
		require.NoError(t, err)

		var calls atomic.Int32
		var wg sync.WaitGroup

		wg.Go(func() {
			l.Maintenance(100*time.Millisecond, f.Name(), stopc, func() (int64, error) {
				calls.Add(1)
				return 0, nil
			})
		})
		stopped := false
		defer func() {
			if !stopped {
				close(stopc)
				wg.Wait()
			}
		}()
		synctest.Wait()

		// Before the first tick, no maintenance executed.
		time.Sleep(99 * time.Millisecond)
		require.EqualValues(t, 0, calls.Load())

		// Tick once.
		time.Sleep(1 * time.Millisecond)
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load())

		// Stop the maintenance loop. We should get exactly one more execution of the maintenance func.
		close(stopc)
		wg.Wait()
		stopped = true

		require.EqualValues(t, 2, calls.Load())
		// Check the maintenance metrics.
		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP alertmanager_nflog_maintenance_errors_total How many maintenances were executed for the notification log that failed.
# TYPE alertmanager_nflog_maintenance_errors_total counter
alertmanager_nflog_maintenance_errors_total 0
# HELP alertmanager_nflog_maintenance_total How many maintenances were executed for the notification log.
# TYPE alertmanager_nflog_maintenance_total counter
alertmanager_nflog_maintenance_total 2
`), "alertmanager_nflog_maintenance_total", "alertmanager_nflog_maintenance_errors_total"))
	})
}

func TestSnapshotEnvelope(t *testing.T) {
	payload := []byte("some length-delimited payload")

	blob := encodeSnapshot(payload)
	require.True(t, bytes.HasPrefix(blob, snapshotMagicV1), "envelope must start with the magic")

	got, err := decodeSnapshotBlob(blob)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Raw bytes without the magic are returned unchanged for snapshots
	// written by older Alertmanager versions.
	legacy := []byte("legacy raw snapshot")
	got, err = decodeSnapshotBlob(legacy)
	require.NoError(t, err)
	require.Equal(t, legacy, got)

	// A flipped checksum byte is detected.
	tampered := bytes.Clone(blob)
	tampered[len(tampered)-1] ^= 0xff
	_, err = decodeSnapshotBlob(tampered)
	require.ErrorIs(t, err, ErrSnapshotCorrupt)

	// A truncated envelope is detected.
	_, err = decodeSnapshotBlob(append(bytes.Clone(snapshotMagicV1), 0x00))
	require.ErrorIs(t, err, ErrSnapshotCorrupt)
}

func newFileLog(t *testing.T, snapshotFile string) *Log {
	t.Helper()
	l, err := New(Options{
		Metrics:      prometheus.NewRegistry(),
		SnapshotFile: snapshotFile,
		Retention:    time.Hour,
	})
	require.NoError(t, err)
	return l
}

func fileLogEntry(groupName, gkey string, ts time.Time) *pb.MeshEntry {
	recv := &pb.Receiver{GroupName: groupName, Integration: "test", Idx: 1}
	return &pb.MeshEntry{
		Entry: &pb.Entry{
			GroupKey:  []byte(gkey),
			Receiver:  recv,
			Timestamp: timestamppb.New(ts),
		},
		ExpiresAt: timestamppb.New(ts.Add(time.Hour)),
	}
}

// TestSnapshotFileCommitAndReload checks that writeSnapshotFile leaves only a
// committed snapshot file and that a fresh Log instance recovers it.
func TestSnapshotFileCommitAndReload(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "nflog")
	l := newFileLog(t, snap)

	now := time.Now()
	e := fileLogEntry("a1", "key", now)
	l.st[stateKey("key", e.Entry.Receiver)] = e

	size, err := l.writeSnapshotFile(snap)
	require.NoError(t, err)
	require.Positive(t, size)
	require.EqualValues(t, 1, l.generation.Load())

	_, err = os.Stat(snapshotCandidatePath(snap))
	require.ErrorIs(t, err, os.ErrNotExist, "candidate must not linger after commit")

	l2 := newFileLog(t, snap)
	entries, err := l2.Query(QGroupKey("key"), QReceiver(e.Entry.Receiver))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, proto.Equal(e.Entry, entries[0]))
}

// TestSnapshotFileEmpty checks that an empty log produces a loadable empty
// snapshot.
func TestSnapshotFileEmpty(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "nflog")
	l := newFileLog(t, snap)

	size, err := l.writeSnapshotFile(snap)
	require.NoError(t, err)
	// Logical payload is empty; the envelope still lands on disk.
	require.Zero(t, size)
	info, err := os.Stat(snap)
	require.NoError(t, err)
	require.Equal(t, int64(len(snapshotMagicV1)+4), info.Size())

	l2 := newFileLog(t, snap)
	require.Empty(t, l2.st)
}

// TestSnapshotFilePrunesExpired checks that expired entries never reach the
// committed snapshot even when GC has not run yet.
func TestSnapshotFilePrunesExpired(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "nflog")
	l := newFileLog(t, snap)

	now := time.Now()
	live := fileLogEntry("live", "g1", now)
	expired := fileLogEntry("expired", "g2", now.Add(-2*time.Hour))
	expired.ExpiresAt = timestamppb.New(now.Add(-time.Second))

	l.st[stateKey("g1", live.Entry.Receiver)] = live
	l.st[stateKey("g2", expired.Entry.Receiver)] = expired

	_, err := l.writeSnapshotFile(snap)
	require.NoError(t, err)

	st, err := verifySnapshotFile(snap)
	require.NoError(t, err)
	require.Contains(t, st, stateKey("g1", live.Entry.Receiver))
	require.NotContains(t, st, stateKey("g2", expired.Entry.Receiver))

	l2 := newFileLog(t, snap)
	_, err = l2.Query(QGroupKey("g2"), QReceiver(expired.Entry.Receiver))
	require.ErrorIs(t, err, ErrNotFound)
}

// TestSnapshotFileFailureKeepsPrevious checks that a failed write does not
// touch the last valid snapshot generation.
func TestSnapshotFileFailureKeepsPrevious(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "nflog")
	l := newFileLog(t, snap)

	e := fileLogEntry("a1", "key", time.Now())
	l.st[stateKey("key", e.Entry.Receiver)] = e
	_, err := l.writeSnapshotFile(snap)
	require.NoError(t, err)

	// A target in a nonexistent directory can never get a candidate.
	bad := filepath.Join(dir, "missing", "nflog")
	_, err = l.writeSnapshotFile(bad)
	require.Error(t, err)

	_, err = os.Stat(snapshotCandidatePath(bad))
	require.ErrorIs(t, err, os.ErrNotExist)

	// The previous generation still loads.
	l2 := newFileLog(t, snap)
	entries, err := l2.Query(QGroupKey("key"), QReceiver(e.Entry.Receiver))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestSnapshotFileRecoverCompletedCandidate simulates a crash after the
// candidate was fully written and synced but before it replaced the current
// file: restart promotes the candidate.
func TestSnapshotFileRecoverCompletedCandidate(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "nflog")

	// First generation on disk, containing the older entry.
	l := newFileLog(t, snap)
	old := fileLogEntry("a1", "key", time.Now().Add(-time.Hour))
	// The older notification must not look expired: its entry timestamp is
	// older while retention keeps it alive.
	old.ExpiresAt = timestamppb.New(time.Now().Add(time.Hour))
	l.st[stateKey("key", old.Entry.Receiver)] = old
	_, err := l.writeSnapshotFile(snap)
	require.NoError(t, err)

	// A completed candidate carrying the newer generation.
	next := fileLogEntry("a1", "key", time.Now())
	payload, err := state{stateKey("key", next.Entry.Receiver): next}.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(snapshotCandidatePath(snap), encodeSnapshot(payload), 0o600))

	l2 := newFileLog(t, snap)

	_, err = os.Stat(snapshotCandidatePath(snap))
	require.ErrorIs(t, err, os.ErrNotExist, "candidate must be resolved on restart")

	entries, err := l2.Query(QGroupKey("key"), QReceiver(next.Entry.Receiver))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, proto.Equal(next.Entry, entries[0]), "newer candidate generation must be recovered")

	// Another restart stays on the promoted generation.
	l3 := newFileLog(t, snap)
	entries, err = l3.Query(QGroupKey("key"), QReceiver(next.Entry.Receiver))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, proto.Equal(next.Entry, entries[0]))
}

// TestSnapshotFileDiscardCorruptCandidate checks that a torn candidate write
// is discarded on restart while the previous valid snapshot is preserved.
func TestSnapshotFileDiscardCorruptCandidate(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "nflog")

	l := newFileLog(t, snap)
	old := fileLogEntry("a1", "key", time.Now().Add(-time.Hour))
	old.ExpiresAt = timestamppb.New(time.Now().Add(time.Hour))
	l.st[stateKey("key", old.Entry.Receiver)] = old
	_, err := l.writeSnapshotFile(snap)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(snapshotCandidatePath(snap), []byte("partial write"), 0o600))

	l2 := newFileLog(t, snap)

	_, err = os.Stat(snapshotCandidatePath(snap))
	require.ErrorIs(t, err, os.ErrNotExist, "corrupt candidate must be removed on restart")

	entries, err := l2.Query(QGroupKey("key"), QReceiver(old.Entry.Receiver))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, proto.Equal(old.Entry, entries[0]), "previous valid snapshot must be preserved")

	// A candidate with a valid envelope but undecodable payload is rejected
	// by the verification step itself.
	bad := encodeSnapshot([]byte("not a protobuf stream"))
	require.NoError(t, os.WriteFile(snapshotCandidatePath(snap), bad, 0o600))
	_, err = verifySnapshotFile(snapshotCandidatePath(snap))
	require.Error(t, err)
}

// TestSnapshotFileLegacyFormats checks that raw snapshots and random temp
// files left by older Alertmanager versions are handled on restart.
func TestSnapshotFileLegacyFormats(t *testing.T) {
	t.Run("raw snapshot loads", func(t *testing.T) {
		snap := filepath.Join(t.TempDir(), "nflog")

		e := fileLogEntry("a1", "key", time.Now())
		st := state{stateKey("key", e.Entry.Receiver): e}
		f, err := os.Create(snap)
		require.NoError(t, err)
		_, err = (&Log{st: st.clone(), metrics: newMetrics(nil)}).Snapshot(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		l := newFileLog(t, snap)
		entries, err := l.Query(QGroupKey("key"), QReceiver(e.Entry.Receiver))
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.True(t, proto.Equal(e.Entry, entries[0]))
	})

	t.Run("stale random temp is removed", func(t *testing.T) {
		dir := t.TempDir()
		snap := filepath.Join(dir, "nflog")

		require.NoError(t, os.WriteFile(snap+".deadbeef", []byte("crashed temp"), 0o600))
		require.NoError(t, os.WriteFile(snap+".bak", []byte("operator backup"), 0o600))

		_ = newFileLog(t, snap)

		_, err := os.Stat(snap + ".deadbeef")
		require.ErrorIs(t, err, os.ErrNotExist, "legacy temp file must be cleaned up")
		_, err = os.Stat(snap + ".bak")
		require.NoError(t, err, "operator-managed files must be left alone")
	})
}

// TestMaintenanceWritesRecoverableSnapshot runs the real maintenance loop and
// checks that a fresh Log instance resumes from its snapshot file.
func TestMaintenanceWritesRecoverableSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snap := filepath.Join(t.TempDir(), "nflog")
		stopc := make(chan struct{})
		reg := prometheus.NewPedanticRegistry()

		l, err := New(Options{
			Metrics:      reg,
			SnapshotFile: snap,
			Retention:    time.Hour,
		})
		require.NoError(t, err)

		recv := &pb.Receiver{GroupName: "a1", Integration: "test", Idx: 1}
		require.NoError(t, l.Log(recv, "key", []uint64{1}, nil, nil, nil, 0))

		var wg sync.WaitGroup
		wg.Go(func() {
			l.Maintenance(100*time.Millisecond, snap, stopc, nil)
		})
		synctest.Wait()
		time.Sleep(101 * time.Millisecond)
		synctest.Wait()
		close(stopc)
		wg.Wait()

		_, err = os.Stat(snapshotCandidatePath(snap))
		require.ErrorIs(t, err, os.ErrNotExist)

		l2 := newFileLog(t, snap)
		entries, err := l2.Query(QGroupKey("key"), QReceiver(recv))
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, []uint64{1}, entries[0].FiringAlerts)
	})
}

func TestStateMerge(t *testing.T) {
	now := time.Now()

	// We only care about key names and timestamps for the
	// merging logic.
	newEntry := func(name string, ts, exp time.Time) *pb.MeshEntry {
		return &pb.MeshEntry{
			Entry: &pb.Entry{
				Timestamp: timestamppb.New(ts),
				GroupKey:  []byte("key"),
				Receiver: &pb.Receiver{
					GroupName:   name,
					Idx:         1,
					Integration: "integr",
				},
			},
			ExpiresAt: timestamppb.New(exp),
		}
	}

	exp := now.Add(time.Minute)

	cases := []struct {
		a, b  state
		final state
	}{
		{
			a: state{
				"key:a1/integr/1": newEntry("a1", now, exp),
				"key:a2/integr/1": newEntry("a2", now, exp),
				"key:a3/integr/1": newEntry("a3", now, exp),
			},
			b: state{
				"key:b1/integr/1": newEntry("b1", now, exp),                                          // new key, should be added
				"key:b2/integr/1": newEntry("b2", now.Add(-time.Minute), now.Add(-time.Millisecond)), // new key, expired, should not be added
				"key:a2/integr/1": newEntry("a2", now.Add(-time.Minute), exp),                        // older timestamp, should be dropped
				"key:a3/integr/1": newEntry("a3", now.Add(time.Minute), exp),                         // newer timestamp, should overwrite
			},
			final: state{
				"key:a1/integr/1": newEntry("a1", now, exp),
				"key:a2/integr/1": newEntry("a2", now, exp),
				"key:a3/integr/1": newEntry("a3", now.Add(time.Minute), exp),
				"key:b1/integr/1": newEntry("b1", now, exp),
			},
		},
	}

	for _, c := range cases {
		ca, cb := c.a.clone(), c.b.clone()

		res := c.a.clone()
		for _, e := range cb {
			res.merge(e, now)
		}
		require.Equal(t, c.final, res, "Merge result should match expectation")
		require.Equal(t, c.b, cb, "Merged state should remain unmodified")
		require.NotEqual(t, c.final, ca, "Merge should not change original state")
	}
}

func TestStateDataCoding(t *testing.T) {
	// Check whether encoding and decoding the data is symmetric.
	now := time.Now().UTC()

	cases := []struct {
		entries []*pb.MeshEntry
	}{
		{
			entries: []*pb.MeshEntry{
				{
					Entry: &pb.Entry{
						GroupKey:  []byte("d8e8fca2dc0f896fd7cb4cb0031ba249"),
						Receiver:  &pb.Receiver{GroupName: "abc", Integration: "test1", Idx: 1},
						GroupHash: []byte("126a8a51b9d1bbd07fddc65819a542c3"),
						Resolved:  false,
						Timestamp: timestamppb.New(now),
					},
					ExpiresAt: timestamppb.New(now),
				}, {
					Entry: &pb.Entry{
						GroupKey:  []byte("d8e8fca2dc0f8abce7cb4cb0031ba249"),
						Receiver:  &pb.Receiver{GroupName: "def", Integration: "test2", Idx: 29},
						GroupHash: []byte("122c2331b9d1bbd07fddc65819a542c3"),
						Resolved:  true,
						Timestamp: timestamppb.New(now),
					},
					ExpiresAt: timestamppb.New(now),
				}, {
					Entry: &pb.Entry{
						GroupKey:  []byte("aaaaaca2dc0f896fd7cb4cb0031ba249"),
						Receiver:  &pb.Receiver{GroupName: "ghi", Integration: "test3", Idx: 0},
						GroupHash: []byte("126a8a51b9d1bbd07fddc6e3e3e542c3"),
						Resolved:  false,
						Timestamp: timestamppb.New(now),
					},
					ExpiresAt: timestamppb.New(now),
				},
			},
		},
	}

	for _, c := range cases {
		// Create gossip data from input.
		in := state{}
		for _, e := range c.entries {
			in[stateKey(string(e.Entry.GroupKey), e.Entry.Receiver)] = e
		}
		msg, err := in.MarshalBinary()
		require.NoError(t, err)

		out, err := decodeState(bytes.NewReader(msg))
		require.NoError(t, err, "decoding message failed")

		for id, expected := range in {
			actual, ok := out[id]
			require.True(t, ok, "silence %s missing from decoded state", id)
			require.True(t, proto.Equal(expected, actual), "silence %s mismatch after decoding", id)
		}
	}
}

func TestQuery(t *testing.T) {
	opts := Options{Metrics: prometheus.NewRegistry(), Retention: time.Second}
	nl, err := New(opts)
	if err != nil {
		require.NoError(t, err, "constructing nflog failed")
	}

	recv := new(pb.Receiver)

	// no key param
	_, err = nl.Query(QGroupKey("key"))
	require.EqualError(t, err, "no query parameters specified")

	// no recv param
	_, err = nl.Query(QReceiver(recv))
	require.EqualError(t, err, "no query parameters specified")

	// no entry
	_, err = nl.Query(QGroupKey("nonexistentkey"), QReceiver(recv))
	require.EqualError(t, err, "not found")

	// existing entry
	firingAlerts := []uint64{1, 2, 3}
	resolvedAlerts := []uint64{4, 5}
	mutedAlerts := []uint64{6, 7}

	err = nl.Log(recv, "key", firingAlerts, resolvedAlerts, mutedAlerts, nil, 0)
	require.NoError(t, err, "logging notification failed")

	entries, err := nl.Query(QGroupKey("key"), QReceiver(recv))
	require.NoError(t, err, "querying nflog failed")
	entry := entries[0]
	require.Equal(t, firingAlerts, entry.FiringAlerts)
	require.Equal(t, resolvedAlerts, entry.ResolvedAlerts)
	require.Equal(t, mutedAlerts, entry.MutedAlerts)
}

// TestLogMutedAlerts checks that muted alerts survive a round trip through the
// wire format, and that an entry logged without them decodes with the field
// unset, as it does for an entry written by a peer that does not know the
// field.
func TestLogMutedAlerts(t *testing.T) {
	now := time.Now().UTC()

	cases := []struct {
		name  string
		muted []uint64
	}{
		{name: "with muted alerts", muted: []uint64{6, 7}},
		{name: "without muted alerts", muted: nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := state{
				"key:abc/test/1": &pb.MeshEntry{
					Entry: &pb.Entry{
						GroupKey:       []byte("key"),
						Receiver:       &pb.Receiver{GroupName: "abc", Integration: "test", Idx: 1},
						Timestamp:      timestamppb.New(now),
						FiringAlerts:   []uint64{1, 2, 3},
						ResolvedAlerts: []uint64{4, 5},
						MutedAlerts:    c.muted,
					},
					ExpiresAt: timestamppb.New(now.Add(time.Minute)),
				},
			}

			msg, err := in.MarshalBinary()
			require.NoError(t, err)

			out, err := decodeState(bytes.NewReader(msg))
			require.NoError(t, err, "decoding message failed")

			for id, expected := range in {
				actual, ok := out[id]
				require.True(t, ok, "entry %s missing from decoded state", id)
				require.True(t, proto.Equal(expected, actual), "entry %s mismatch after decoding", id)
				require.Equal(t, c.muted, actual.Entry.MutedAlerts)
			}
		})
	}
}

// TestStateMergeMutedAlerts checks that merging stays timestamp-based: the
// newer entry replaces the older one wholesale, muted alerts included, with no
// per-field merging of the alert lists.
func TestStateMergeMutedAlerts(t *testing.T) {
	now := time.Now()

	newEntry := func(ts time.Time, muted []uint64) *pb.MeshEntry {
		return &pb.MeshEntry{
			Entry: &pb.Entry{
				Timestamp:   timestamppb.New(ts),
				GroupKey:    []byte("key"),
				Receiver:    &pb.Receiver{GroupName: "a1", Idx: 1, Integration: "integr"},
				MutedAlerts: muted,
			},
			ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		}
	}

	const key = "key:a1/integr/1"

	// A newer entry without muted alerts replaces an older one that has them.
	res := state{key: newEntry(now, []uint64{1, 2})}
	res.merge(newEntry(now.Add(time.Minute), nil), now)
	require.Empty(t, res[key].Entry.MutedAlerts)

	// An older entry is dropped, so its muted alerts do not resurface.
	res = state{key: newEntry(now, nil)}
	res.merge(newEntry(now.Add(-time.Minute), []uint64{1, 2}), now)
	require.Empty(t, res[key].Entry.MutedAlerts)
}

func TestStateDecodingError(t *testing.T) {
	// Check whether decoding copes with erroneous data.
	s := state{"": &pb.MeshEntry{}}

	msg, err := s.MarshalBinary()
	require.NoError(t, err)

	_, err = decodeState(bytes.NewReader(msg))
	require.Equal(t, ErrInvalidState, err)
}

// TestMergeNeverRollsBack checks the cluster merge path: entries from an old
// snapshot neither roll a current entry back nor resurrect expired or
// malformed entries, while genuinely newer entries are accepted.
func TestMergeNeverRollsBack(t *testing.T) {
	now := time.Now()

	recv := func(name string) *pb.Receiver {
		return &pb.Receiver{GroupName: name, Integration: "test", Idx: 1}
	}
	entry := func(name string, ts, exp time.Time) *pb.MeshEntry {
		return &pb.MeshEntry{
			Entry: &pb.Entry{
				GroupKey:  []byte("key"),
				Receiver:  recv(name),
				Timestamp: timestamppb.New(ts),
			},
			ExpiresAt: timestamppb.New(exp),
		}
	}

	encode := func(entries ...*pb.MeshEntry) []byte {
		st := state{}
		for _, e := range entries {
			st[stateKey("key", e.Entry.Receiver)] = e
		}
		b, err := st.MarshalBinary()
		require.NoError(t, err)
		return b
	}

	t.Run("older, equal, expired and zero-expiry entries are rejected", func(t *testing.T) {
		l, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)

		current := entry("a1", now, now.Add(time.Hour))
		l.st[stateKey("key", current.Entry.Receiver)] = current

		older := entry("a1", now.Add(-time.Minute), now.Add(time.Hour))
		equal := entry("a1", now, now.Add(time.Hour))
		expiredNewKey := entry("a2", now.Add(-time.Hour), now.Add(-time.Millisecond))
		zeroExpiry := entry("a3", now.Add(time.Minute), time.Time{})

		require.NoError(t, l.Merge(encode(older, equal, expiredNewKey, zeroExpiry)))

		got := l.st[stateKey("key", current.Entry.Receiver)]
		require.True(t, proto.Equal(current, got), "current entry must not be rolled back")
		require.NotContains(t, l.st, stateKey("key", recv("a2")), "expired entry must not be resurrected")
		require.NotContains(t, l.st, stateKey("key", recv("a3")), "zero-expiry entry must be rejected")
	})

	t.Run("newer entry is accepted and stays queryable", func(t *testing.T) {
		l, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)

		current := entry("a1", now, now.Add(time.Hour))
		l.st[stateKey("key", current.Entry.Receiver)] = current

		newer := entry("a1", now.Add(time.Minute), now.Add(2*time.Hour))
		require.NoError(t, l.Merge(encode(newer)))

		entries, qerr := l.Query(QGroupKey("key"), QReceiver(recv("a1")))
		require.NoError(t, qerr)
		require.Len(t, entries, 1)
		require.True(t, proto.Equal(newer.Entry, entries[0]))
	})

	t.Run("new unexpired key is accepted", func(t *testing.T) {
		l, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)

		fresh := entry("a1", now, now.Add(time.Hour))
		require.NoError(t, l.Merge(encode(fresh)))

		entries, qerr := l.Query(QGroupKey("key"), QReceiver(recv("a1")))
		require.NoError(t, qerr)
		require.Len(t, entries, 1)
	})
}
