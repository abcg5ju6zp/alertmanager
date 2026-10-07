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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/prometheus/alertmanager/eventrecorder"
	"github.com/prometheus/alertmanager/labelset"
	"github.com/prometheus/alertmanager/marker"
	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

var errSimulatedCrash = errors.New("simulated crash")

func newFileSilences(t *testing.T, retention time.Duration) (*Silences, string) {
	t.Helper()
	snap := filepath.Join(t.TempDir(), "silences")
	s, err := New(Options{
		Metrics:      prometheus.NewRegistry(),
		Retention:    retention,
		SnapshotFile: snap,
		Logger:       promslog.NewNopLogger(),
	})
	require.NoError(t, err)
	return s, snap
}

func reopenFileSilences(t *testing.T, snap string, retention time.Duration) *Silences {
	t.Helper()
	s, err := New(Options{
		Metrics:      prometheus.NewRegistry(),
		Retention:    retention,
		SnapshotFile: snap,
		Logger:       promslog.NewNopLogger(),
	})
	require.NoError(t, err)
	return s
}

func mustLoadSnapshotState(t *testing.T, path string) state {
	t.Helper()
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return state{}
	}
	require.NoError(t, err)
	defer f.Close()
	st, err := decodeState(f)
	require.NoError(t, err)
	return st
}

func eqMatcherSet(name, pattern string) []*pb.MatcherSet {
	return []*pb.MatcherSet{{
		Matchers: []*pb.Matcher{{
			Type:    pb.Matcher_EQUAL,
			Name:    name,
			Pattern: pattern,
		}},
	}}
}

func silenceProto(name, pattern string, start, end time.Time) *pb.Silence {
	return &pb.Silence{
		MatcherSets: eqMatcherSet(name, pattern),
		StartsAt:    timestamppb.New(start),
		EndsAt:      timestamppb.New(end),
	}
}

func assertCommittedStateConsistent(t *testing.T, s *Silences, snap string) {
	t.Helper()
	requireStatesEqual(t, mustLoadSnapshotState(t, snap), s.st, "snapshot file and committed state diverged")

	ids := map[string]struct{}{}
	for id := range s.st {
		ids[id] = struct{}{}
		_, ok := s.mi[id]
		require.True(t, ok, "matcher index missing silence %s", id)
	}
	for id := range s.mi {
		_, ok := ids[id]
		require.True(t, ok, "matcher index contains stale silence %s", id)
	}
	viIDs := map[string]struct{}{}
	for _, sv := range s.vi {
		viIDs[sv.id] = struct{}{}
		_, ok := ids[sv.id]
		require.True(t, ok, "version index contains stale silence %s", sv.id)
	}
	require.Len(t, viIDs, len(ids), "version index does not cover all state entries")
}

func queryAllIDs(t *testing.T, s *Silences) map[string]struct{} {
	t.Helper()
	res, _, err := s.Query(context.Background())
	require.NoError(t, err)
	ids := make(map[string]struct{}, len(res))
	for _, sil := range res {
		ids[sil.Id] = struct{}{}
	}
	return ids
}

// TestSilenceTransactionalCommit verifies AC-1: create/update/expire are only
// visible once the durable snapshot contains them, and state, matcher index
// and version index stay consistent at every step.
func TestSilenceTransactionalCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, snap := newFileSilences(t, time.Hour)
	now := s.nowUTC()

	sil := silenceProto("foo", "bar", now.Add(-time.Minute), now.Add(time.Hour))
	require.NoError(t, s.Set(ctx, sil))
	require.NotEmpty(t, sil.Id)
	version := s.Version()
	require.GreaterOrEqual(t, version, 1)
	assertCommittedStateConsistent(t, s, snap)

	got, err := s.QueryOne(ctx, QIDs(sil.Id))
	require.NoError(t, err)
	require.True(t, proto.Equal(sil, got), "query result should match the created silence")

	// An in-place update keeps the same ID and is snapshot-confirmed too.
	updated := cloneSilence(sil)
	updated.EndsAt = timestamppb.New(now.Add(2 * time.Hour))
	require.NoError(t, s.Set(ctx, updated))
	require.GreaterOrEqual(t, s.Version(), version)
	assertCommittedStateConsistent(t, s, snap)
	got, err = s.QueryOne(ctx, QIDs(sil.Id))
	require.NoError(t, err)
	require.Equal(t, updated.EndsAt.AsTime().Unix(), got.EndsAt.AsTime().Unix())

	require.NoError(t, s.Expire(ctx, sil.Id))
	assertCommittedStateConsistent(t, s, snap)
	got, err = s.QueryOne(ctx, QIDs(sil.Id))
	require.NoError(t, err)
	require.Equal(t, SilenceStateExpired, getState(got, s.nowUTC()))
}

// TestCommitFailureKeepsLastConfirmedSet verifies AC-2: when the durable
// snapshot fails, the commit keeps the previous confirmed set in memory,
// queries and the notification pipeline never observe the candidate, and the
// candidate is continued on recovery.
func TestCommitFailureKeepsLastConfirmedSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, snap := newFileSilences(t, 0)
	now := s.nowUTC()

	// A confirmed silence that is already past its GC deadline.
	s1 := silenceProto("foo", "bar", now.Add(-2*time.Hour), now.Add(-time.Hour))
	require.NoError(t, s.Set(ctx, s1))
	baseline := mustLoadSnapshotState(t, snap)
	baselineVersion := s.Version()

	silencer := NewSilencer(s, promslog.NewNopLogger(), eventrecorder.NopRecorder())
	mutes := func(set model.LabelSet) bool {
		m := marker.NewAlertMarker()
		return silencer.Mutes(marker.WithContext(context.Background(), m), labelset.FromModel(set))
	}

	s.snapshotFault = func() error { return errSimulatedCrash }

	// A new silence must not become visible.
	s2 := silenceProto("baz", "qux", now, now.Add(time.Hour))
	err := s.Set(ctx, s2)
	require.ErrorIs(t, err, errSimulatedCrash)

	ids := queryAllIDs(t, s)
	require.Len(t, ids, 1)
	require.Contains(t, ids, s1.Id)
	require.Equal(t, baselineVersion, s.Version(), "version must not advance on failed commit")
	require.Len(t, s.mi, 1)
	require.Len(t, s.vi, 1)
	requireStatesEqual(t, baseline, mustLoadSnapshotState(t, snap), "snapshot must stay at the last confirmed set")
	matched, _, err := s.Query(ctx, QMatches(model.LabelSet{"baz": "qux"}))
	require.NoError(t, err)
	require.Empty(t, matched, "matcher index must not expose the unconfirmed silence")
	require.False(t, mutes(model.LabelSet{"baz": "qux"}), "notification pipeline must not observe the unconfirmed silence")
	require.Len(t, s.jrnl.unconfirmed(), 1, "the failed upsert must remain a recoverable candidate")

	// GC must not publish deletions when the snapshot cannot be confirmed.
	n, err := s.GC()
	require.ErrorIs(t, err, errSimulatedCrash)
	require.Equal(t, 0, n)
	require.Contains(t, queryAllIDs(t, s), s1.Id)
	requireStatesEqual(t, baseline, mustLoadSnapshotState(t, snap))
	require.Len(t, s.jrnl.unconfirmed(), 2, "failed upsert and GC should both be recoverable candidates")

	// Recovery continues both candidates: s2 is committed and s1 is gone.
	s2store := reopenFileSilences(t, snap, 0)
	ids = queryAllIDs(t, s2store)
	require.Len(t, ids, 1)
	require.Contains(t, ids, s2.Id)
	require.NotContains(t, ids, s1.Id)
	assertCommittedStateConsistent(t, s2store, snap)
	require.Empty(t, s2store.jrnl.unconfirmed())

	silencer2 := NewSilencer(s2store, promslog.NewNopLogger(), eventrecorder.NopRecorder())
	m2 := marker.NewAlertMarker()
	require.True(t, silencer2.Mutes(marker.WithContext(context.Background(), m2), labelset.FromModel(model.LabelSet{"baz": "qux"})))
}

// TestJournalRecoveryCrashPoints verifies AC-3: crashing after PENDING is
// synced or after the snapshot rename but before CONFIRMED leads, on restart,
// to exactly the state the completed operation would have produced, and
// repeated restarts are idempotent.
func TestJournalRecoveryCrashPoints(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		stage string
	}{
		{name: "upsert after pending", stage: commitStagePending},
		{name: "upsert after snapshot", stage: commitStageSnapshot},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			s, snap := newFileSilences(t, time.Hour)
			now := s.nowUTC()

			// A confirmed existing silence.
			a := silenceProto("a", "1", now.Add(-time.Minute), now.Add(time.Hour))
			require.NoError(t, s.Set(ctx, a))

			crashStage := tc.stage
			s.crashHook = func(stage string) error {
				if stage == crashStage {
					return errSimulatedCrash
				}
				return nil
			}

			b := silenceProto("b", "2", now.Add(-time.Minute), now.Add(time.Hour))
			err := s.Set(ctx, b)
			require.ErrorIs(t, err, errSimulatedCrash)
			require.NotEmpty(t, b.Id)

			recovered := reopenFileSilences(t, snap, time.Hour)
			require.Contains(t, queryAllIDs(t, recovered), a.Id)
			require.Contains(t, queryAllIDs(t, recovered), b.Id)
			got, err := recovered.QueryOne(ctx, QIDs(b.Id))
			require.NoError(t, err)
			require.True(t, proto.Equal(b, got))
			assertCommittedStateConsistent(t, recovered, snap)
			require.Empty(t, recovered.jrnl.unconfirmed())

			// A second restart must not change anything.
			recoveredAgain := reopenFileSilences(t, snap, time.Hour)
			requireStatesEqual(t, recovered.st, recoveredAgain.st)
			require.Empty(t, recoveredAgain.jrnl.unconfirmed())
		})
	}

	t.Run("gc after pending", func(t *testing.T) {
		t.Parallel()

		s, snap := newFileSilences(t, 0)
		now := s.nowUTC()
		c := silenceProto("c", "1", now.Add(-2*time.Hour), now.Add(-time.Hour))
		require.NoError(t, s.Set(context.Background(), c))

		s.crashHook = func(stage string) error {
			if stage == commitStagePending {
				return errSimulatedCrash
			}
			return nil
		}
		_, err := s.GC()
		require.ErrorIs(t, err, errSimulatedCrash)
		require.Contains(t, queryAllIDs(t, s), c.Id, "unpublished GC must keep the silence visible")

		recovered := reopenFileSilences(t, snap, 0)
		require.NotContains(t, queryAllIDs(t, recovered), c.Id, "tombstone candidate must be continued on recovery")
		assertCommittedStateConsistent(t, recovered, snap)
		require.Empty(t, recovered.jrnl.unconfirmed())

		recoveredAgain := reopenFileSilences(t, snap, 0)
		require.NotContains(t, queryAllIDs(t, recoveredAgain), c.Id)
		require.Empty(t, recoveredAgain.jrnl.unconfirmed())
	})

	t.Run("gc after snapshot", func(t *testing.T) {
		t.Parallel()

		s, snap := newFileSilences(t, 0)
		now := s.nowUTC()
		c := silenceProto("c", "1", now.Add(-2*time.Hour), now.Add(-time.Hour))
		require.NoError(t, s.Set(context.Background(), c))

		s.crashHook = func(stage string) error {
			if stage == commitStageSnapshot {
				return errSimulatedCrash
			}
			return nil
		}
		_, err := s.GC()
		require.ErrorIs(t, err, errSimulatedCrash)

		recovered := reopenFileSilences(t, snap, 0)
		require.NotContains(t, queryAllIDs(t, recovered), c.Id, "snapshot-confirmed deletion must survive the restart")
		require.Empty(t, recovered.jrnl.unconfirmed())
	})
}

// TestGCTombstoneRecovery verifies AC-4: successful and failed GC commits
// never resurrect deleted silences after a restart.
func TestGCTombstoneRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, snap := newFileSilences(t, 0)
	now := s.nowUTC()
	x := silenceProto("x", "1", now.Add(-2*time.Hour), now.Add(-time.Hour))
	require.NoError(t, s.Set(ctx, x))

	n, err := s.GC()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NotContains(t, queryAllIDs(t, s), x.Id)
	require.NotContains(t, mustLoadSnapshotState(t, snap), x.Id)

	reopened := reopenFileSilences(t, snap, 0)
	require.NotContains(t, queryAllIDs(t, reopened), x.Id)

	// A snapshot failure leaves the entry visible, but recovery continues the
	// tombstone candidate.
	y := silenceProto("y", "1", now.Add(-2*time.Hour), now.Add(-time.Hour))
	require.NoError(t, s.Set(ctx, y))
	s.snapshotFault = func() error { return errSimulatedCrash }
	_, err = s.GC()
	require.ErrorIs(t, err, errSimulatedCrash)
	require.Contains(t, queryAllIDs(t, s), y.Id)

	reopened2 := reopenFileSilences(t, snap, 0)
	require.NotContains(t, queryAllIDs(t, reopened2), y.Id)
	assertCommittedStateConsistent(t, reopened2, snap)
}

// TestGCKeepsReferencedSilences verifies AC-4: GC must not delete a silence
// still referenced by an unconfirmed newer candidate.
func TestGCKeepsReferencedSilences(t *testing.T) {
	t.Parallel()

	s, snap := newFileSilences(t, 0)
	now := s.nowUTC()

	x := silenceProto("foo", "bar", now.Add(-2*time.Hour), now.Add(-time.Hour))
	require.NoError(t, s.Set(context.Background(), x))

	// Simulate a durable unconfirmed update that extends the same silence.
	refreshed := &pb.MeshSilence{
		Silence: &pb.Silence{
			Id:          x.Id,
			MatcherSets: eqMatcherSet("foo", "bar"),
			StartsAt:    timestamppb.New(now),
			EndsAt:      timestamppb.New(now.Add(time.Hour)),
			UpdatedAt:   timestamppb.New(now.Add(time.Millisecond)),
		},
		ExpiresAt: timestamppb.New(now.Add(time.Hour)),
	}
	s.mtx.Lock()
	_, err := s.jrnl.appendUpsert(refreshed)
	s.mtx.Unlock()
	require.NoError(t, err)

	n, err := s.GC()
	require.NoError(t, err)
	require.Equal(t, 0, n, "referenced silence must not be collected")
	got, err := s.QueryOne(context.Background(), QIDs(x.Id))
	require.NoError(t, err)
	// The unconfirmed candidate is not visible yet: the committed entry is
	// still the expired one, but its matcher index entry must be intact.
	require.Equal(t, now.Add(-time.Hour).Unix(), got.EndsAt.AsTime().Unix())
	matched, _, err := s.Query(context.Background(), QMatches(model.LabelSet{"foo": "bar"}))
	require.NoError(t, err)
	require.Len(t, matched, 1)

	// Recovery continues the refresh candidate and the silence becomes active.
	recovered := reopenFileSilences(t, snap, time.Hour)
	got, err = recovered.QueryOne(context.Background(), QIDs(x.Id))
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Hour).Unix(), got.EndsAt.AsTime().Unix())
	require.Equal(t, SilenceStateActive, getState(got, recovered.nowUTC()))
	active, _, err := recovered.Query(context.Background(), QState(SilenceStateActive), QMatches(model.LabelSet{"foo": "bar"}))
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Empty(t, recovered.jrnl.unconfirmed())
}

// TestConvergenceTieBreak verifies AC-6: newer UpdatedAt wins and equal
// timestamps converge by deterministic content comparison regardless of
// arrival order.
func TestConvergenceTieBreak(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	mk := func(ts time.Time, comment string) *pb.MeshSilence {
		return &pb.MeshSilence{
			Silence: &pb.Silence{
				Id:          "id",
				Comment:     comment,
				MatcherSets: eqMatcherSet("foo", "bar"),
				StartsAt:    timestamppb.New(now),
				EndsAt:      timestamppb.New(now.Add(time.Hour)),
				UpdatedAt:   timestamppb.New(ts),
			},
			ExpiresAt: timestamppb.New(now.Add(time.Hour)),
		}
	}

	older := mk(now, "a")
	newer := mk(now.Add(time.Minute), "b")
	require.True(t, silenceNewer(older, newer))
	require.False(t, silenceNewer(newer, older))
	require.False(t, silenceNewer(older, older), "identical entries should not replace each other")

	sameTimeA := mk(now, "a")
	sameTimeB := mk(now, "b")
	// Deterministic outcome independent of argument order.
	winner := silenceNewer(sameTimeA, sameTimeB)
	require.Equal(t, winner, !silenceNewer(sameTimeB, sameTimeA))

	// state.merge must converge identically under both arrival orders.
	mergeOnce := func(first, second *pb.MeshSilence) *pb.MeshSilence {
		st := state{}
		st.merge(first, now)
		st.merge(second, now)
		return st["id"]
	}
	w1 := mergeOnce(sameTimeA, sameTimeB)
	w2 := mergeOnce(sameTimeB, sameTimeA)
	require.True(t, proto.Equal(w1, w2), "equal timestamps must converge regardless of arrival order")
	require.Equal(t, map[bool]string{true: "b", false: "a"}[winner], w1.Silence.Comment)
}

// TestMergeTransactionalBroadcast verifies AC-7: gossip frames keep the
// legacy length-delimited MeshSilence format, merges are durable and
// deduplicated, matcher changes propagate, and snapshot failures reject the
// merge and leave local state untouched.
func TestMergeTransactionalBroadcast(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	a, snapA := newFileSilences(t, time.Hour)
	b, snapB := newFileSilences(t, time.Hour)

	var aBroadcast [][]byte
	a.broadcast = func(frame []byte) { aBroadcast = append(aBroadcast, frame) }
	var bBroadcast [][]byte
	b.broadcast = func(frame []byte) { bBroadcast = append(bBroadcast, frame) }

	now := a.nowUTC()
	sil := silenceProto("foo", "bar", now.Add(-time.Minute), now.Add(time.Hour))
	require.NoError(t, a.Set(ctx, sil))
	require.Len(t, aBroadcast, 1)

	// The wire format must be decodable by the legacy stream decoder.
	decoded, err := decodeState(bytes.NewReader(aBroadcast[0]))
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	incoming := decoded[sil.Id]
	require.True(t, proto.Equal(a.st[sil.Id], incoming))

	// First merge applies, persists and re-propagates once.
	require.NoError(t, b.Merge(aBroadcast[0]))
	got, err := b.QueryOne(ctx, QIDs(sil.Id))
	require.NoError(t, err)
	require.True(t, proto.Equal(sil, got))
	assertCommittedStateConsistent(t, b, snapB)
	require.Len(t, bBroadcast, 1)
	matched, _, err := b.Query(ctx, QMatches(model.LabelSet{"foo": "bar"}))
	require.NoError(t, err)
	require.Len(t, matched, 1)

	// The same batch again is a no-op with no extra propagation or version.
	versionBefore := b.Version()
	require.NoError(t, b.Merge(aBroadcast[0]))
	require.Equal(t, versionBefore, b.Version())
	require.Len(t, bBroadcast, 1, "identical gossip must not be re-propagated")

	// A gossip message with changed matchers for the same ID rebuilds the
	// matcher index.
	changed := cloneMeshSilence(incoming)
	changed.Silence = cloneSilence(incoming.Silence)
	changed.Silence.MatcherSets = eqMatcherSet("foo", "zzz")
	changed.Silence.UpdatedAt = timestamppb.New(now.Add(time.Minute))
	frame, err := marshalMeshSilence(changed)
	require.NoError(t, err)
	require.NoError(t, b.Merge(frame))
	oldMatch, _, err := b.Query(ctx, QMatches(model.LabelSet{"foo": "bar"}))
	require.NoError(t, err)
	require.Empty(t, oldMatch)
	newMatch, _, err := b.Query(ctx, QMatches(model.LabelSet{"foo": "zzz"}))
	require.NoError(t, err)
	require.Len(t, newMatch, 1)

	// MarshalBinary exposes only confirmed entries in the legacy format.
	all, err := b.MarshalBinary()
	require.NoError(t, err)
	full, err := decodeState(bytes.NewReader(all))
	require.NoError(t, err)
	requireStatesEqual(t, b.st, full)

	// Snapshot failure rejects the merge without local effects or gossip.
	b.snapshotFault = func() error { return errSimulatedCrash }
	otherSil := silenceProto("keep", "me", now.Add(-time.Minute), now.Add(time.Hour))
	otherSil.Id = "keep-id"
	otherFrame, err := marshalMeshSilence(&pb.MeshSilence{
		Silence:   otherSil,
		ExpiresAt: timestamppb.New(now.Add(time.Hour)),
	})
	require.NoError(t, err)
	beforeCount := len(b.st)
	beforeBroadcast := len(bBroadcast)
	err = b.Merge(otherFrame)
	require.ErrorIs(t, err, errSimulatedCrash)
	require.Len(t, b.st, beforeCount)
	require.Len(t, bBroadcast, beforeBroadcast)

	// Recovery continues the gossip candidate exactly like a local one.
	recovered := reopenFileSilences(t, snapB, time.Hour)
	got, err = recovered.QueryOne(ctx, QIDs("keep-id"))
	require.NoError(t, err)
	require.True(t, proto.Equal(otherSil, got))
	assertCommittedStateConsistent(t, recovered, snapB)
	require.Empty(t, recovered.jrnl.unconfirmed())

	// A's recovery path keeps its own committed set intact.
	reopenedA := reopenFileSilences(t, snapA, time.Hour)
	assertCommittedStateConsistent(t, reopenedA, snapA)
}

// TestInMemoryModeHasNoJournal verifies AC-9 for reader-based construction.
func TestInMemoryModeHasNoJournal(t *testing.T) {
	t.Parallel()

	s, err := New(Options{
		Metrics:        prometheus.NewRegistry(),
		SnapshotReader: bytes.NewReader(nil),
		Logger:         promslog.NewNopLogger(),
	})
	require.NoError(t, err)
	require.Nil(t, s.jrnl)
	require.Empty(t, s.snapFile)
}

// TestRecoverLegacySnapshot verifies AC-9: an old-format snapshot using the
// deprecated matchers and comments fields upgrades cleanly and enables
// transactional commits afterwards.
func TestRecoverLegacySnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	snap := filepath.Join(dir, "silences")
	now := time.Now().UTC()

	legacy := &pb.MeshSilence{
		Silence: &pb.Silence{
			Id: "legacy-id",
			// Old format: only the repeated matchers field plus comments.
			Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "foo", Pattern: "bar"}},
			Comments: []*pb.Comment{{
				Author:    "oncall",
				Comment:   "old comment",
				Timestamp: timestamppb.New(now),
			}},
			StartsAt:  timestamppb.New(now.Add(-time.Minute)),
			EndsAt:    timestamppb.New(now.Add(time.Hour)),
			UpdatedAt: timestamppb.New(now),
		},
		ExpiresAt: timestamppb.New(now.Add(time.Hour)),
	}
	frame, err := marshalMeshSilence(legacy)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(snap, frame, 0o644))

	s := reopenFileSilences(t, snap, time.Hour)
	got, err := s.QueryOne(context.Background(), QIDs("legacy-id"))
	require.NoError(t, err)
	require.Equal(t, "old comment", got.Comment)
	require.Equal(t, "oncall", got.CreatedBy)
	require.Len(t, got.MatcherSets, 1)

	matched, _, err := s.Query(context.Background(), QMatches(model.LabelSet{"foo": "bar"}))
	require.NoError(t, err)
	require.Len(t, matched, 1)
	require.FileExists(t, snap+journalFileSuffix)

	// The on-disk snapshot keeps the legacy bytes until the first commit
	// rewrites it in the upgraded, still pure MeshSilence stream format.
	second := silenceProto("baz", "qux", now.Add(-time.Minute), now.Add(time.Hour))
	require.NoError(t, s.Set(context.Background(), second))
	upgraded := mustLoadSnapshotState(t, snap)
	require.Len(t, upgraded, 2)
	legacyEntry := upgraded["legacy-id"].Silence
	require.Equal(t, "old comment", legacyEntry.Comment)
	require.Equal(t, "oncall", legacyEntry.CreatedBy)
	require.Empty(t, legacyEntry.Comments)
	require.Len(t, legacyEntry.MatcherSets, 1)
	assertCommittedStateConsistent(t, s, snap)
}

// TestMaxSilencesCountsUnconfirmedCandidates verifies I-2/F-2: a candidate
// stranded by a failed snapshot commit occupies capacity, so a retry cannot
// exceed the configured silence limit.
func TestMaxSilencesCountsUnconfirmedCandidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	snap := filepath.Join(t.TempDir(), "silences")
	s, err := New(Options{
		Metrics: prometheus.NewRegistry(),
		Limits: Limits{
			MaxSilences: func() int { return 2 },
		},
		SnapshotFile: snap,
		Logger:       promslog.NewNopLogger(),
	})
	require.NoError(t, err)
	now := s.nowUTC()

	first := silenceProto("foo", "bar", now.Add(-time.Minute), now.Add(time.Hour))
	require.NoError(t, s.Set(ctx, first))

	// A failed create leaves a durable candidate but no committed entry.
	s.snapshotFault = func() error { return errSimulatedCrash }
	stranded := silenceProto("baz", "qux", now.Add(-time.Minute), now.Add(time.Hour))
	require.ErrorIs(t, s.Set(ctx, stranded), errSimulatedCrash)
	s.snapshotFault = nil

	// A retry must be rejected because the working set already holds one
	// committed and one unconfirmed silence.
	retry := silenceProto("keep", "me", now.Add(-time.Minute), now.Add(time.Hour))
	err = s.Set(ctx, retry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded maximum number of silences")

	// Recovery admits the stranded candidate (accepted before the crash);
	// the limit is intentionally not re-enforced on replay.
	recovered := reopenFileSilencesWithLimit(t, snap, func() int { return 2 })
	ids := queryAllIDs(t, recovered)
	require.Len(t, ids, 2)
	require.Contains(t, ids, first.Id)
	require.Contains(t, ids, stranded.Id)
}

func reopenFileSilencesWithLimit(t *testing.T, snap string, max func() int) *Silences {
	t.Helper()
	s, err := New(Options{
		Metrics:      prometheus.NewRegistry(),
		Limits:       Limits{MaxSilences: max},
		SnapshotFile: snap,
		Logger:       promslog.NewNopLogger(),
	})
	require.NoError(t, err)
	return s
}

// TestRecoveryEqualTimestampTieBreak verifies the convergence rule is applied
// on the journal replay path too: a pending candidate with the same UpdatedAt
// as the snapshot entry only replaces it when its content deterministically
// wins, regardless of how the candidate arrived.
func TestRecoveryEqualTimestampTieBreak(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	snap := filepath.Join(dir, "silences")
	now := time.Now().UTC()

	mk := func(comment string) *pb.MeshSilence {
		return &pb.MeshSilence{
			Silence: &pb.Silence{
				Id:          "tie-id",
				Comment:     comment,
				MatcherSets: eqMatcherSet("foo", "bar"),
				StartsAt:    timestamppb.New(now.Add(-time.Minute)),
				EndsAt:      timestamppb.New(now.Add(time.Hour)),
				UpdatedAt:   timestamppb.New(now),
			},
			ExpiresAt: timestamppb.New(now.Add(time.Hour)),
		}
	}

	// Seed the snapshot directly with the "a" variant.
	frameA, err := marshalMeshSilence(mk("a"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(snap, frameA, 0o644))

	// Append an unconfirmed "b" candidate with the same UpdatedAt.
	jrnl, err := openJournal(snap + journalFileSuffix)
	require.NoError(t, err)
	_, err = jrnl.appendUpsert(mk("b"))
	require.NoError(t, err)
	require.NoError(t, jrnl.close())

	bShouldWin := silenceNewer(mk("a"), mk("b"))

	recovered := reopenFileSilences(t, snap, time.Hour)
	got, err := recovered.QueryOne(context.Background(), QIDs("tie-id"))
	require.NoError(t, err)
	if bShouldWin {
		require.Equal(t, "b", got.Comment)
	} else {
		require.Equal(t, "a", got.Comment)
	}
	require.Empty(t, recovered.jrnl.unconfirmed())

	// The reverse order converges to the very same winner.
	require.NoError(t, os.Remove(snap+journalFileSuffix))
	frameB, err := marshalMeshSilence(mk("b"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(snap, frameB, 0o644))
	jrnl2, err := openJournal(snap + journalFileSuffix)
	require.NoError(t, err)
	_, err = jrnl2.appendUpsert(mk("a"))
	require.NoError(t, err)
	require.NoError(t, jrnl2.close())

	recovered2 := reopenFileSilences(t, snap, time.Hour)
	got2, err := recovered2.QueryOne(context.Background(), QIDs("tie-id"))
	require.NoError(t, err)
	require.Equal(t, got.Comment, got2.Comment, "replay must converge regardless of candidate order")
}

// TestMaintenanceCheckpointCompactsJournal verifies the journal is compacted
// after a maintenance snapshot while candidates are retained.
func TestMaintenanceCheckpointCompactsJournal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, snap := newFileSilences(t, time.Hour)
	now := s.nowUTC()
	sil := silenceProto("foo", "bar", now.Add(-time.Minute), now.Add(time.Hour))
	require.NoError(t, s.Set(ctx, sil))
	require.Equal(t, uint64(0), s.jrnl.baseline())

	// A closed stop channel makes Maintenance run its final maintenance
	// body once and return.
	stopc := make(chan struct{})
	close(stopc)
	s.Maintenance(time.Hour, snap, stopc, nil)

	require.Positive(t, s.jrnl.baseline())
	require.Empty(t, s.jrnl.unconfirmed())
	assertCommittedStateConsistent(t, s, snap)

	// The compacted journal is still a valid baseline after reopening.
	reopened := reopenFileSilences(t, snap, time.Hour)
	require.Contains(t, queryAllIDs(t, reopened), sil.Id)
	require.Equal(t, s.jrnl.baseline(), reopened.jrnl.baseline())
}
