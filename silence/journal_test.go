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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

func testMeshSilence(id string) *pb.MeshSilence {
	return &pb.MeshSilence{
		Silence: &pb.Silence{
			Id:      id,
			Comment: "journal test " + id,
			MatcherSets: []*pb.MatcherSet{{
				Matchers: []*pb.Matcher{{
					Name:    "foo",
					Pattern: "bar",
					Type:    pb.Matcher_EQUAL,
				}},
			}},
			StartsAt:  timestamppb.Now(),
			EndsAt:    timestamppb.Now(),
			UpdatedAt: timestamppb.Now(),
		},
	}
}

func TestJournalAppendAndReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "silences.journal")

	j, err := openJournal(path)
	require.NoError(t, err)
	require.Equal(t, uint64(0), j.baseline())

	ups1 := testMeshSilence("id-1")
	seq1, err := j.appendUpsert(ups1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq1)

	gcIDs := []string{"id-a", "id-b"}
	seq2, err := j.appendGC(gcIDs)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq2)

	ups3 := testMeshSilence("id-3")
	seq3, err := j.appendUpsert(ups3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), seq3)

	require.NoError(t, j.appendConfirmed(seq1))

	unconfirmed := j.unconfirmed()
	require.Len(t, unconfirmed, 2)
	require.Equal(t, uint64(2), unconfirmed[0].seq)
	require.True(t, unconfirmed[0].gc)
	require.Equal(t, gcIDs, unconfirmed[0].ids)
	require.Equal(t, uint64(3), unconfirmed[1].seq)
	require.True(t, proto.Equal(ups3, unconfirmed[1].silence), "upsert payload should survive a copy")

	require.NoError(t, j.close())

	// Reopen: state must be reconstructed from the file.
	j2, err := openJournal(path)
	require.NoError(t, err)
	require.False(t, j2.hadTornTail())
	require.Equal(t, uint64(0), j2.baseline())
	require.Equal(t, uint64(4), j2.nextSequence())

	unconfirmed2 := j2.unconfirmed()
	require.Len(t, unconfirmed2, 2)
	require.Equal(t, uint64(2), unconfirmed2[0].seq)
	require.True(t, unconfirmed2[0].gc)
	require.Equal(t, gcIDs, unconfirmed2[0].ids)
	require.Equal(t, uint64(3), unconfirmed2[1].seq)
	require.True(t, proto.Equal(ups3, unconfirmed2[1].silence))

	// Mutating the returned silence must not corrupt journal state.
	unconfirmed2[1].silence.Silence.Comment = "changed"
	unconfirmed3 := j2.unconfirmed()
	require.Equal(t, "journal test id-3", unconfirmed3[1].silence.Silence.Comment)

	require.NoError(t, j2.close())
}

func TestJournalTruncatedAndCorruptTail(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		damage  func(t *testing.T, data []byte) []byte
		wantOps int
	}{
		{
			name: "header torn",
			damage: func(_ *testing.T, data []byte) []byte {
				// Remove part of the first frame header but keep the file header.
				return data[:len(journalFileMagic)+2]
			},
			wantOps: 0,
		},
		{
			name: "payload torn",
			damage: func(_ *testing.T, data []byte) []byte {
				return data[:len(data)-10]
			},
			wantOps: 1,
		},
		{
			name: "crc mismatch",
			damage: func(_ *testing.T, data []byte) []byte {
				// Flip a payload byte in the last frame.
				idx := len(data) - 8
				data[idx] ^= 0xff
				return data
			},
			wantOps: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "silences.journal")
			j, err := openJournal(path)
			require.NoError(t, err)
			_, err = j.appendUpsert(testMeshSilence("id-1"))
			require.NoError(t, err)
			_, err = j.appendGC([]string{"id-2"})
			require.NoError(t, err)
			require.NoError(t, j.close())

			data, err := os.ReadFile(path)
			require.NoError(t, err)
			data = tc.damage(t, data)
			require.NoError(t, os.WriteFile(path, data, 0o644))

			j2, err := openJournal(path)
			require.NoError(t, err)
			require.True(t, j2.hadTornTail(), "torn tail should be reported")

			ops := j2.unconfirmed()
			require.Len(t, ops, tc.wantOps)

			// Appending after a torn tail must succeed and stay parseable.
			_, err = j2.appendUpsert(testMeshSilence("id-3"))
			require.NoError(t, err)
			require.NoError(t, j2.close())

			j3, err := openJournal(path)
			require.NoError(t, err)
			require.False(t, j3.hadTornTail())
			require.Len(t, j3.unconfirmed(), tc.wantOps+1)
			require.NoError(t, j3.close())
		})
	}
}

func TestJournalBadMagic(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "silences.journal")
	require.NoError(t, os.WriteFile(path, []byte("NOTAJNL!!garbagegarbage"), 0o644))

	_, err := openJournal(path)
	require.Error(t, err)
}

func TestJournalCheckpoint(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "silences.journal")
	j, err := openJournal(path)
	require.NoError(t, err)

	seq1, err := j.appendUpsert(testMeshSilence("id-1"))
	require.NoError(t, err)
	seq2, err := j.appendGC([]string{"id-2"})
	require.NoError(t, err)
	seq3, err := j.appendUpsert(testMeshSilence("id-3"))
	require.NoError(t, err)

	require.NoError(t, j.appendConfirmed(seq1))
	require.NoError(t, j.appendConfirmed(seq2))

	// Checkpointing through seq2 must keep the still-pending seq3.
	require.NoError(t, j.checkpoint(seq2))
	require.Equal(t, uint64(2), j.baseline())
	unconfirmed := j.unconfirmed()
	require.Len(t, unconfirmed, 1)
	require.Equal(t, uint64(3), unconfirmed[0].seq)
	require.NoError(t, j.close())

	// Reopen and verify the compacted journal reconstructs identically.
	j2, err := openJournal(path)
	require.NoError(t, err)
	require.False(t, j2.hadTornTail())
	require.Equal(t, uint64(2), j2.baseline())
	require.Equal(t, uint64(4), j2.nextSequence())
	require.Len(t, j2.unconfirmed(), 1)
	require.Equal(t, uint64(3), j2.unconfirmed()[0].seq)

	// A new record keeps monotonically increasing sequences.
	seq4, err := j2.appendGC([]string{"id-4"})
	require.NoError(t, err)
	require.Equal(t, uint64(4), seq4)

	// Checkpointing through seq4 empties the pending set.
	require.NoError(t, j2.appendConfirmed(seq3))
	require.NoError(t, j2.appendConfirmed(seq4))
	require.NoError(t, j2.checkpoint(seq4))
	require.Empty(t, j2.unconfirmed())
	require.Equal(t, uint64(4), j2.baseline())
	require.NoError(t, j2.close())

	j3, err := openJournal(path)
	require.NoError(t, err)
	require.Equal(t, uint64(4), j3.baseline())
	require.Equal(t, uint64(5), j3.nextSequence())
	require.Empty(t, j3.unconfirmed())
	require.NoError(t, j3.close())
}

func TestJournalCheckpointRejectsUnconfirmed(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "silences.journal")
	j, err := openJournal(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.close()) })

	seq, err := j.appendUpsert(testMeshSilence("id-1"))
	require.NoError(t, err)
	require.Error(t, j.checkpoint(seq))
	require.Equal(t, uint64(0), j.baseline())
}
