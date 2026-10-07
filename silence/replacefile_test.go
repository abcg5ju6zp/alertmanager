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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// TestReplaceFileAbortKeepsConfirmedSnapshot verifies that a failed snapshot
// write never renames an empty or partial temporary file over the last
// confirmed snapshot.
func TestReplaceFileAbortKeepsConfirmedSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	snap := filepath.Join(dir, "silences")
	original := []byte("CONFIRMED-SNAPSHOT")
	require.NoError(t, os.WriteFile(snap, original, 0o644))

	f, err := openReplace(snap)
	require.NoError(t, err)

	// Close the underlying descriptor to force every subsequent write to
	// fail deterministically without ever calling Sync or Rename. A
	// non-empty state is required so the writer actually issues a write.
	require.NoError(t, f.File.Close())
	st := state{"x": {
		Silence: &pb.Silence{
			Id:          "x",
			MatcherSets: []*pb.MatcherSet{{Matchers: []*pb.Matcher{{Name: "a", Pattern: "b"}}}},
			StartsAt:    timestamppb.Now(),
			EndsAt:      timestamppb.Now(),
		},
	}}
	_, err = snapshotState(f, st)
	require.Error(t, err, "writing to the closed descriptor must fail")

	require.NoError(t, f.abort())

	got, err := os.ReadFile(snap)
	require.NoError(t, err)
	require.Equal(t, original, got, "the confirmed snapshot must remain untouched")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the aborted temporary file must be removed")
}

// TestMaintenanceSnapshotFailureKeepsFile verifies that a maintenance run
// which cannot write its snapshot leaves the existing snapshot intact.
func TestMaintenanceSnapshotFailureKeepsFile(t *testing.T) {
	t.Parallel()

	s, err := New(Options{
		Metrics: prometheus.NewRegistry(),
		Logger:  promslog.NewNopLogger(),
	})
	require.NoError(t, err)

	dir := t.TempDir()
	snap := filepath.Join(dir, "silences")
	original := []byte("CONFIRMED-SNAPSHOT")
	require.NoError(t, os.WriteFile(snap, original, 0o644))
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	// A closed stop channel makes Maintenance run its final maintenance
	// body once and return; the snapshot failure is logged, not surfaced.
	stopc := make(chan struct{})
	close(stopc)
	s.Maintenance(time.Hour, snap, stopc, nil)

	require.NoError(t, os.Chmod(dir, 0o755))
	got, err := os.ReadFile(snap)
	require.NoError(t, err)
	require.Equal(t, original, got, "the failed maintenance snapshot must not replace the file")
}
