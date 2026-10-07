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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// TestSilenceTransactionalConcurrent exercises concurrent creates, expires,
// gossip merges and GC runs against one store and verifies that the committed
// structures never diverge from each other or from the durable snapshot.
func TestSilenceTransactionalConcurrent(t *testing.T) {
	t.Parallel()

	s, snap := newFileSilences(t, 24*time.Hour)
	ctx := context.Background()
	now := s.nowUTC()

	const creators = 16
	var wg sync.WaitGroup
	var creatorsWG sync.WaitGroup
	creatorsWG.Add(creators)

	// Concurrent creates with unique label names and IDs returned to the
	// other workers.
	createdIDs := make(chan string, creators)
	for i := range creators {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer creatorsWG.Done()
			sil := &pb.Silence{
				MatcherSets: []*pb.MatcherSet{{
					Matchers: []*pb.Matcher{{
						Type:    pb.Matcher_EQUAL,
						Name:    fmt.Sprintf("label_%d", i),
						Pattern: "value",
					}},
				}},
				StartsAt: timestamppb.New(now.Add(-time.Minute)),
				EndsAt:   timestamppb.New(now.Add(time.Hour)),
			}
			if err := s.Set(ctx, sil); err != nil {
				t.Errorf("create %d: %v", i, err)
				return
			}
			createdIDs <- sil.Id
		}(i)
	}

	// Concurrent gossip merges for one contended ID with increasing
	// timestamps: the final winner must be deterministic by UpdatedAt.
	const mergers = 8
	contendedID := "gossip-contended"
	for i := range mergers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msil := &pb.MeshSilence{
				Silence: &pb.Silence{
					Id:          contendedID,
					Comment:     fmt.Sprintf("v%d", i),
					MatcherSets: eqMatcherSet("contended", fmt.Sprintf("v%d", i)),
					StartsAt:    timestamppb.New(now.Add(-time.Minute)),
					EndsAt:      timestamppb.New(now.Add(time.Hour)),
					UpdatedAt:   timestamppb.New(now.Add(time.Duration(i) * time.Second)),
				},
				ExpiresAt: timestamppb.New(now.Add(25 * time.Hour)),
			}
			frame, err := marshalMeshSilence(msil)
			if err != nil {
				t.Errorf("marshal %d: %v", i, err)
				return
			}
			if err := s.Merge(frame); err != nil {
				t.Errorf("merge %d: %v", i, err)
			}
		}(i)
	}

	// GC workers: nothing is GC-eligible with the chosen retention, but they
	// still exercise the full transactional GC path concurrently.
	for range 4 {
		wg.Go(func() {
			if _, err := s.GC(); err != nil {
				t.Errorf("gc: %v", err)
			}
		})
	}

	// Expire workers race on the created silences once creation finishes.
	creatorsWG.Wait()
	close(createdIDs)
	var ids []string
	for id := range createdIDs {
		ids = append(ids, id)
	}
	for _, id := range ids[:creators/2] {
		wg.Go(func() {
			if err := s.Expire(ctx, id); err != nil {
				t.Errorf("expire: %v", err)
			}
		})
	}

	wg.Wait()

	// The contended gossip ID must carry the highest UpdatedAt version.
	winner, err := s.QueryOne(ctx, QIDs(contendedID))
	require.NoError(t, err)
	require.Equal(t, now.Add((mergers-1)*time.Second).Unix(), winner.UpdatedAt.AsTime().Unix())
	require.Equal(t, "v7", winner.Comment)
	matched, _, err := s.Query(ctx, QMatches(model.LabelSet{"contended": "v7"}))
	require.NoError(t, err)
	require.Len(t, matched, 1)

	assertCommittedStateConsistent(t, s, snap)

	// Restart and verify the recovered set is identical and still coherent.
	reopened := reopenFileSilences(t, snap, 24*time.Hour)
	requireStatesEqual(t, s.st, reopened.st)
	assertCommittedStateConsistent(t, reopened, snap)
	require.Empty(t, reopened.jrnl.unconfirmed())
}

// TestMaintenanceSnapshotSerializedAgainstCommits is a regression test for
// the race where a maintenance snapshot could be renamed over a snapshot a
// concurrent commit had just confirmed: after maintenance stops, a restart
// must observe every committed silence and no unconfirmed candidates.
func TestMaintenanceSnapshotSerializedAgainstCommits(t *testing.T) {
	t.Parallel()

	s, snap := newFileSilences(t, 48*time.Hour)
	ctx := context.Background()
	now := s.nowUTC()

	stopc := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		s.Maintenance(2*time.Millisecond, snap, stopc, nil)
	})

	const total = 60
	ids := make([]string, 0, total)
	for i := range total {
		sil := &pb.Silence{
			MatcherSets: []*pb.MatcherSet{{
				Matchers: []*pb.Matcher{{
					Type:    pb.Matcher_EQUAL,
					Name:    fmt.Sprintf("maint_label_%d", i),
					Pattern: "value",
				}},
			}},
			StartsAt: timestamppb.New(now.Add(-time.Minute)),
			EndsAt:   timestamppb.New(now.Add(24 * time.Hour)),
		}
		require.NoError(t, s.Set(ctx, sil), "creating silence %d", i)
		ids = append(ids, sil.Id)
	}

	// Stopping triggers the final maintenance snapshot, then wait until the
	// maintenance goroutine has finished before reopening the files.
	close(stopc)
	wg.Wait()

	reopened := reopenFileSilences(t, snap, 48*time.Hour)
	gotIDs := queryAllIDs(t, reopened)
	for _, id := range ids {
		require.Contains(t, gotIDs, id, "committed silence %s lost across maintenance snapshot", id)
	}
	require.Empty(t, reopened.jrnl.unconfirmed())
	assertCommittedStateConsistent(t, reopened, snap)
}

// TestMaintenanceSnapshotCriticalSectionExcludesWriters deterministically
// verifies that the maintenance snapshot replacement is serialized against
// committing writers.
func TestMaintenanceSnapshotCriticalSectionExcludesWriters(t *testing.T) {
	t.Parallel()

	s, snap := newFileSilences(t, time.Hour)
	entered := make(chan struct{})
	var once sync.Once
	proceed := make(chan struct{})
	// Probe result reported by the maintenance goroutine while it still
	// holds the snapshot critical section.
	probeResult := make(chan bool, 1)
	s.maintenanceHook = func() {
		once.Do(func() { close(entered) })
		<-proceed
		// Non-blocking: only the first tick is observed; later ticks and
		// the final maintenance run must not hang on a full channel.
		select {
		case probeResult <- s.mtx.TryLock():
		default:
		}
	}

	stopped := make(chan struct{})
	stopc := make(chan struct{})
	go func() {
		defer close(stopped)
		s.Maintenance(5*time.Millisecond, snap, stopc, nil)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance section never started")
	}

	// A writer must block until the maintenance section finishes.
	done := make(chan struct{})
	go func() {
		_ = s.Expire(context.Background(), "missing")
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("writer completed while the maintenance snapshot section was held")
	case <-time.After(50 * time.Millisecond):
	}
	close(proceed)
	select {
	case writerAcquired := <-probeResult:
		require.False(t, writerAcquired, "a writer acquired the lock during the maintenance snapshot section")
		if writerAcquired {
			s.mtx.Unlock()
		}
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance hook never reported the probe result")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer remained blocked after the maintenance section finished")
	}
	close(stopc)
	<-stopped
}
