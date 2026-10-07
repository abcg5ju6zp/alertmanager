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
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// Journal commit stage names used by the crash-injection test hooks.
const (
	commitStagePending  = "pending"
	commitStageSnapshot = "snapshot"
)

// workState is a private, not-yet-published copy of the silence state. All
// candidate operations are applied to a workState first; nothing in it is
// visible to queries or the notification pipeline until publish replaces the
// committed structures in one step.
type workState struct {
	st      state
	mi      matcherIndex
	vi      versionIndex
	version int
}

// newWorkState returns an independent working copy of the committed state.
// Callers must hold s.mtx.
func (s *Silences) newWorkState() *workState {
	ws := &workState{
		st:      make(state, len(s.st)+1),
		mi:      make(matcherIndex, len(s.mi)+1),
		vi:      make(versionIndex, 0, len(s.vi)+1),
		version: s.version,
	}
	maps.Copy(ws.st, s.st)
	maps.Copy(ws.mi, s.mi)
	ws.vi = append(ws.vi, s.vi...)
	return ws
}

// upsert applies an incoming silence entry to the working copy following the
// time-then-content convergence rule. It reports whether the entry added a
// new ID, replaced an existing one, or was ignored because the committed
// working copy already holds the same or a newer version.
func (ws *workState) upsert(e *pb.MeshSilence, s *Silences) (added, replaced bool) {
	id := e.Silence.Id
	prev, ok := ws.st[id]
	if ok && !silenceNewer(prev, e) {
		return false, false
	}
	added = !ok
	replaced = ok

	if replaced {
		// The matcher index entry must track the new content whenever the
		// matcher sets changed. Updates that only touch timestamps keep the
		// already compiled matcher set.
		if !slices.EqualFunc(prev.Silence.MatcherSets, e.Silence.MatcherSets, func(a, b *pb.MatcherSet) bool {
			return proto.Equal(a, b)
		}) {
			delete(ws.mi, id)
		}
	}

	ws.st[id] = e
	if added {
		ws.version++
		ws.vi.add(ws.version, id)
	}
	if _, ok := ws.mi[id]; !ok {
		if _, err := ws.mi.add(e.Silence); err != nil {
			s.metrics.matcherCompileIndexSilenceErrorsTotal.Inc()
			s.logger.Error("Failed to compile silence matchers", "silence_id", id, "err", err)
		}
	}
	return added, replaced
}

// gcPlan describes the deletions a GC pass derived from a working copy.
type gcPlan struct {
	ids  []string
	n    int
	errs error
}

// planGC removes silences past their retention deadline from the working
// copy. IDs listed in referenced are still claimed by an unconfirmed newer
// version and must be retained. Callers must hold s.mtx.
func (ws *workState) planGC(now time.Time, referenced map[string]struct{}, s *Silences) *gcPlan {
	plan := &gcPlan{}

	// Mirror the committed version index reallocation policy so that the
	// published index keeps its shrink behaviour.
	needsRealloc := cap(ws.vi) > 1024 && len(ws.vi) < cap(ws.vi)/2
	var targetVi versionIndex
	if needsRealloc {
		newCap := max(len(ws.vi)*5/4, 1024)
		targetVi = make(versionIndex, 0, newCap)
	} else {
		targetVi = ws.vi[:0]
	}

	for _, sv := range ws.vi {
		sil, ok := ws.st[sv.id]
		if !ok {
			s.metrics.gcErrorsTotal.Inc()
			plan.errs = errors.Join(plan.errs, fmt.Errorf("silence %s in version index missing from state", sv.id))
			continue
		}
		if _, claimed := referenced[sv.id]; claimed {
			// An unconfirmed candidate still references this ID; the
			// deletion would be undone during recovery and must not be
			// published.
			targetVi = append(targetVi, sv)
			continue
		}
		expire := false
		if sil.ExpiresAt == nil || sil.ExpiresAt.AsTime().IsZero() {
			s.metrics.gcErrorsTotal.Inc()
			plan.errs = errors.Join(plan.errs, fmt.Errorf("silence %s has zero expiration timestamp", sil.Silence.Id))
			expire = true
		}
		if expire || !sil.ExpiresAt.AsTime().After(now) {
			delete(ws.st, sv.id)
			delete(ws.mi, sv.id)
			plan.ids = append(plan.ids, sv.id)
			plan.n++
		} else {
			targetVi = append(targetVi, sv)
		}
	}

	if !needsRealloc {
		clear(ws.vi[len(targetVi):])
	}
	ws.vi = targetVi
	return plan
}

// publish atomically makes the working copy the committed state. Callers
// must hold s.mtx.
func (s *Silences) publish(ws *workState) {
	s.st = ws.st
	s.mi = ws.mi
	s.vi = ws.vi
	s.version = ws.version
	s.updateSizeMetrics()
}

// pendingReferences returns the IDs an unconfirmed journal candidate last
// touched, classified as upserts (referenced) rather than GC deletions.
// Callers must hold s.mtx.
func (s *Silences) pendingReferences() map[string]struct{} {
	refs := map[string]struct{}{}
	if s.jrnl == nil {
		return refs
	}
	// Operations are returned in sequence order; a later upsert reclaims an
	// ID that an earlier GC candidate deleted, and vice versa.
	for _, op := range s.jrnl.unconfirmed() {
		if op.gc {
			for _, id := range op.ids {
				delete(refs, id)
			}
			continue
		}
		refs[op.silence.Silence.Id] = struct{}{}
	}
	return refs
}

// pendingNewSilenceCount reports how many distinct silence IDs are
// referenced by unconfirmed upsert candidates but not part of the committed
// state yet. Callers must hold s.mtx.
func (s *Silences) pendingNewSilenceCount() int {
	if s.jrnl == nil {
		return 0
	}
	pendingIDs := map[string]struct{}{}
	for _, op := range s.jrnl.unconfirmed() {
		if op.gc {
			continue
		}
		id := op.silence.Silence.Id
		if _, committed := s.st[id]; !committed {
			pendingIDs[id] = struct{}{}
		}
	}
	return len(pendingIDs)
}

// publishDurable runs the commit pipeline. PENDING records for upserts and
// GC deletions are made durable first, then the resulting full state is
// written to the snapshot file atomically, and only then is the working
// copy published to queries and the notification pipeline. CONFIRMED
// records are written last. In journal-less mode the working copy is
// published directly, preserving the historical in-memory behaviour.
//
// Callers must hold s.mtx.
func (s *Silences) publishDurable(ws *workState, upserts []*pb.MeshSilence, gcIDs []string) error {
	if s.jrnl == nil {
		s.publish(ws)
		return nil
	}

	seqs := make([]uint64, 0, len(upserts)+1)
	for _, e := range upserts {
		seq, err := s.jrnl.appendUpsert(e)
		if err != nil {
			return err
		}
		seqs = append(seqs, seq)
	}
	if len(gcIDs) > 0 {
		seq, err := s.jrnl.appendGC(gcIDs)
		if err != nil {
			return err
		}
		seqs = append(seqs, seq)
	}

	if s.crashHook != nil {
		if err := s.crashHook(commitStagePending); err != nil {
			return err
		}
	}

	if s.snapshotFault != nil {
		if err := s.snapshotFault(); err != nil {
			return fmt.Errorf("persist silence snapshot: %w", err)
		}
	} else {
		if err := s.writeDurableSnapshot(ws.st); err != nil {
			return err
		}
	}

	if s.crashHook != nil {
		if err := s.crashHook(commitStageSnapshot); err != nil {
			return err
		}
	}

	s.publish(ws)

	var confirmErr error
	for _, seq := range seqs {
		if err := s.jrnl.appendConfirmed(seq); err != nil {
			confirmErr = errors.Join(confirmErr, err)
		}
	}
	return confirmErr
}

// writeDurableSnapshot writes st to the configured snapshot file using a
// temporary file, fsync and atomic rename. A failed write or sync aborts the
// temporary file without ever touching the existing snapshot.
func (s *Silences) writeDurableSnapshot(st state) error {
	f, err := openReplace(s.snapFile)
	if err != nil {
		return err
	}
	if _, err := snapshotState(f, st); err != nil {
		_ = f.abort()
		return err
	}
	if err := f.Close(); err != nil {
		_ = f.discard()
		return err
	}
	return nil
}

// snapshotState writes the state to w in the legacy length-delimited
// MeshSilence record stream format.
func snapshotState(w io.Writer, st state) (int64, error) {
	b, err := st.MarshalBinary()
	if err != nil {
		return 0, err
	}
	return io.Copy(w, bytes.NewReader(b))
}

// deterministicSilenceMarshal encodes a silence with deterministic field
// ordering so that equal values always produce equal bytes.
var deterministicSilenceMarshal = proto.MarshalOptions{Deterministic: true}

// silenceNewer reports whether candidate should win over the previously
// stored entry. Newer UpdatedAt wins; at equal timestamps the lexicograph
// comparison of their deterministic encodings decides, which makes
// convergence independent of arrival order. Equal entries never win.
func silenceNewer(prev, candidate *pb.MeshSilence) bool {
	prevTime := prev.Silence.UpdatedAt.AsTime()
	candTime := candidate.Silence.UpdatedAt.AsTime()
	if candTime.After(prevTime) {
		return true
	}
	if candTime.Before(prevTime) {
		return false
	}
	prevBytes, err := deterministicSilenceMarshal.Marshal(prev.Silence)
	if err != nil {
		return false
	}
	candBytes, err := deterministicSilenceMarshal.Marshal(candidate.Silence)
	if err != nil {
		return false
	}
	return bytes.Compare(candBytes, prevBytes) > 0
}

// applyGCCandidates deletes the listed IDs from the working copy when they
// are still past their expiration deadline. It reports whether anything was
// removed. IDs referenced by a later upsert are re-added by the caller's
// replay loop.
func (ws *workState) applyGCCandidates(ids []string, now time.Time) bool {
	drop := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		e, ok := ws.st[id]
		if !ok {
			continue
		}
		if e.ExpiresAt == nil || !e.ExpiresAt.AsTime().After(now) {
			drop[id] = struct{}{}
			delete(ws.st, id)
			delete(ws.mi, id)
		}
	}
	if len(drop) == 0 {
		return false
	}
	filtered := make(versionIndex, 0, len(ws.vi))
	for _, sv := range ws.vi {
		if _, ok := drop[sv.id]; !ok {
			filtered = append(filtered, sv)
		}
	}
	ws.vi = filtered
	return true
}

// recoverCandidates continues PENDING journal operations that never reached a
// CONFIRMED record. The operations are replayed in sequence order onto the
// loaded snapshot baseline, one snapshot is durably replaced, the result is
// published and every continued operation is confirmed and compacted. It is
// only called from New before the store serves any traffic.
func (s *Silences) recoverCandidates() error {
	if s.jrnl == nil {
		return nil
	}
	if s.jrnl.hadTornTail() {
		s.logger.Warn("Found a torn silence journal tail; recovered up to the last intact record")
	}

	ops := s.jrnl.unconfirmed()
	if len(ops) == 0 {
		return nil
	}
	s.logger.Info("Recovering unconfirmed silence operations", "count", len(ops))

	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := s.nowUTC()
	ws := s.newWorkState()
	seqs := make([]uint64, 0, len(ops))

	for _, op := range ops {
		seqs = append(seqs, op.seq)
		if op.gc {
			ws.applyGCCandidates(op.ids, now)
			continue
		}
		// Payloads originate from our own marshaling, but normalize
		// defensively so older fields are upgraded exactly like snapshots.
		postprocessUnmarshalledSilence(op.silence.Silence)
		if op.silence.ExpiresAt.AsTime().Before(now) {
			// The candidate has aged past its GC deadline; convergence
			// drops it like an expired gossip message.
			continue
		}
		// Convergence makes replay idempotent when the snapshot already
		// contains the candidate: equal time and content is ignored.
		ws.upsert(op.silence, s)
	}

	if err := s.writeDurableSnapshot(ws.st); err != nil {
		return fmt.Errorf("persist recovered silence snapshot: %w", err)
	}
	s.publish(ws)

	for _, seq := range seqs {
		if err := s.jrnl.appendConfirmed(seq); err != nil {
			return err
		}
	}
	if err := s.jrnl.checkpointConfirmed(); err != nil {
		return fmt.Errorf("compact silence journal after recovery: %w", err)
	}
	return nil
}
