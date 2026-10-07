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

// Package nflog implements a garbage-collected and snapshottable append-only log of
// active/resolved notifications. Each log entry stores the active/resolved state,
// the notified receiver, and a hash digest of the notification's identifying contents.
// The log can be queried along different parameters.
package nflog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/promslog"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/prometheus/alertmanager/cluster"
	pb "github.com/prometheus/alertmanager/nflog/nflogpb"
)

// ErrNotFound is returned for empty query results.
var ErrNotFound = errors.New("not found")

// ErrInvalidState is returned if the state isn't valid.
var ErrInvalidState = errors.New("invalid state")

// ErrSnapshotCorrupt is returned if a snapshot file fails its integrity
// check.
var ErrSnapshotCorrupt = errors.New("corrupt notification log snapshot")

// snapshotMagicV1 prefixes every crash-recoverable snapshot generation. A
// snapshot file is laid out as:
//
//	magic | payload | CRC-32 checksum
//
// The payload is the same length-delimited stream of MeshEntry messages used
// by the cluster protocol. The magic keeps the envelope distinguishable from
// snapshots written by older Alertmanager versions, which contain the raw
// payload and keep loading unchanged.
var snapshotMagicV1 = []byte("AMNFLOG1")

// snapshotCandidateSuffix is appended to the snapshot file name while a new
// generation is being encoded and verified. The fixed name lets a restarting
// process resolve the candidate instead of accumulating stale temp files.
const snapshotCandidateSuffix = ".candidate"

// query currently allows filtering by and/or receiver group key.
// It is configured via QueryParameter functions.
//
// TODO(fabxc): Future versions could allow querying a certain receiver,
// group or a given time interval.
type query struct {
	recv     *pb.Receiver
	groupKey string
}

// QueryParam is a function that modifies a query to incorporate
// a set of parameters. Returns an error for invalid or conflicting
// parameters.
type QueryParam func(*query) error

// QReceiver adds a receiver parameter to a query.
func QReceiver(r *pb.Receiver) QueryParam {
	return func(q *query) error {
		q.recv = r
		return nil
	}
}

// QGroupKey adds a group key as querying argument.
func QGroupKey(gk string) QueryParam {
	return func(q *query) error {
		q.groupKey = gk
		return nil
	}
}

// Store abstracts the NFLog's receiver data storage as a mutable key/value store. A store
// can be generated from a nflogpb.Entry and then written via the call to Log.
//
// Every key in the Store is associated with either an int, float, or string value.
type Store struct {
	data map[string]*pb.ReceiverDataValue
}

// NewStore creates a Store from the entry's receiver data. If entry is nil, the resulting
// Store is empty.
func NewStore(entry *pb.Entry) *Store {
	var receiverData map[string]*pb.ReceiverDataValue
	if entry != nil {
		receiverData = maps.Clone(entry.ReceiverData)
	}
	if receiverData == nil {
		receiverData = make(map[string]*pb.ReceiverDataValue)
	}
	return &Store{
		data: receiverData,
	}
}

// GetInt finds the integer value associated with the key, if any, and returns it.
func (s *Store) GetInt(key string) (int64, bool) {
	dataValue, ok := s.data[key]
	if !ok {
		return 0, false
	}
	intVal, ok := dataValue.Value.(*pb.ReceiverDataValue_IntVal)
	if !ok {
		return 0, false
	}
	return intVal.IntVal, true
}

// GetFloat finds the float value associated with the key, if any, and returns it.
func (s *Store) GetFloat(key string) (float64, bool) {
	dataValue, ok := s.data[key]
	if !ok {
		return 0, false
	}
	floatVal, ok := dataValue.Value.(*pb.ReceiverDataValue_DoubleVal)
	if !ok {
		return 0, false
	}
	return floatVal.DoubleVal, true
}

// GetFloat finds the string value associated with the key, if any, and returns it.
func (s *Store) GetStr(key string) (string, bool) {
	dataValue, ok := s.data[key]
	if !ok {
		return "", false
	}
	strVal, ok := dataValue.Value.(*pb.ReceiverDataValue_StrVal)
	if !ok {
		return "", false
	}
	return strVal.StrVal, true
}

// SetInt associates an integer value with the provided key, overwriting any existing value.
func (s *Store) SetInt(key string, v int64) {
	s.data[key] = &pb.ReceiverDataValue{
		Value: &pb.ReceiverDataValue_IntVal{
			IntVal: v,
		},
	}
}

// SetFloat associates a float value with the provided key, overwriting any existing value.
func (s *Store) SetFloat(key string, v float64) {
	s.data[key] = &pb.ReceiverDataValue{
		Value: &pb.ReceiverDataValue_DoubleVal{
			DoubleVal: v,
		},
	}
}

// SetStr associates a string value with the provided key, overwriting any existing value.
func (s *Store) SetStr(key, v string) {
	s.data[key] = &pb.ReceiverDataValue{
		Value: &pb.ReceiverDataValue_StrVal{
			StrVal: v,
		},
	}
}

// Delete deletes any value associated with the key.
func (s *Store) Delete(key string) {
	delete(s.data, key)
}

// Log holds the notification log state for alerts that have been notified.
type Log struct {
	logger    *slog.Logger
	metrics   *metrics
	retention time.Duration

	// For now we only store the most recently added log entry.
	// The key is a serialized concatenation of group key and receiver.
	mtx       sync.RWMutex
	st        state
	broadcast func([]byte)

	// generation counts snapshot generations that were durably committed
	// to disk. It is only meaningful when a snapshot file is configured.
	generation atomic.Uint64
}

// MaintenanceFunc represents the function to run as part of the periodic maintenance for the nflog.
// It returns the size of the snapshot taken or an error if it failed.
type MaintenanceFunc func() (int64, error)

type metrics struct {
	gcDuration              prometheus.Summary
	snapshotDuration        prometheus.Summary
	snapshotSize            prometheus.Gauge
	queriesTotal            prometheus.Counter
	queryErrorsTotal        prometheus.Counter
	queryDuration           prometheus.Histogram
	propagatedMessagesTotal prometheus.Counter
	maintenanceTotal        prometheus.Counter
	maintenanceErrorsTotal  prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	m := &metrics{}

	m.gcDuration = promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "alertmanager_nflog_gc_duration_seconds",
		Help:       "Duration of the last notification log garbage collection cycle.",
		Objectives: map[float64]float64{},
	})
	m.snapshotDuration = promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "alertmanager_nflog_snapshot_duration_seconds",
		Help:       "Duration of the last notification log snapshot.",
		Objectives: map[float64]float64{},
	})
	m.snapshotSize = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "alertmanager_nflog_snapshot_size_bytes",
		Help: "Size of the last notification log snapshot in bytes.",
	})
	m.maintenanceTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_maintenance_total",
		Help: "How many maintenances were executed for the notification log.",
	})
	m.maintenanceErrorsTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_maintenance_errors_total",
		Help: "How many maintenances were executed for the notification log that failed.",
	})
	m.queriesTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_queries_total",
		Help: "Number of notification log queries were received.",
	})
	m.queryErrorsTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_query_errors_total",
		Help: "Number notification log received queries that failed.",
	})
	m.queryDuration = promauto.With(r).NewHistogram(prometheus.HistogramOpts{
		Name:                            "alertmanager_nflog_query_duration_seconds",
		Help:                            "Duration of notification log query evaluation.",
		Buckets:                         prometheus.DefBuckets,
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	})
	m.propagatedMessagesTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_nflog_gossip_messages_propagated_total",
		Help: "Number of received gossip messages that have been further gossiped.",
	})

	return m
}

type state map[string]*pb.MeshEntry

func (s state) clone() state {
	c := make(state, len(s))
	maps.Copy(c, s)
	return c
}

// merge returns true or false whether the MeshEntry was merged or
// not. This information is used to decide to gossip the message further.
//
// An entry is only ever accepted if it is valid, unexpired and strictly
// newer than the entry currently stored for its key. Entries from an old
// snapshot can therefore neither resurrect a key that garbage collection
// removed nor roll a present entry back to an older version.
func (s state) merge(e *pb.MeshEntry, now time.Time) bool {
	if e == nil || e.Entry == nil || e.Entry.Receiver == nil {
		return false
	}
	expiresAt := e.ExpiresAt.AsTime()
	if expiresAt.IsZero() || expiresAt.Before(now) {
		return false
	}
	k := stateKey(string(e.Entry.GroupKey), e.Entry.Receiver)

	prev, ok := s[k]
	if !ok {
		s[k] = e
		return true
	}
	// Only a strictly newer timestamp may replace the current entry.
	// Equal timestamps are ignored so a retransmitted entry cannot reset
	// an equally new, possibly richer entry.
	if prev.Entry.Timestamp.AsTime().Before(e.Entry.Timestamp.AsTime()) {
		s[k] = e
		return true
	}
	return false
}

func (s state) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	for _, e := range s {
		if _, err := protodelim.MarshalTo(&buf, e); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// marshalBinaryPruned serializes all entries that have not expired at now.
// It never mutates state, which lets a snapshot candidate omit entries that
// expired since the last garbage collection cycle.
func (s state) marshalBinaryPruned(now time.Time) ([]byte, error) {
	var buf bytes.Buffer

	for _, e := range s {
		expiresAt := e.ExpiresAt.AsTime()
		if expiresAt.IsZero() || !expiresAt.After(now) {
			continue
		}
		if _, err := protodelim.MarshalTo(&buf, e); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeState(r io.Reader) (state, error) {
	st := state{}
	br := bufio.NewReader(r)
	for {
		var e pb.MeshEntry
		err := protodelim.UnmarshalFrom(br, &e)
		if err == nil {
			if e.Entry == nil || e.Entry.Receiver == nil {
				return nil, ErrInvalidState
			}
			st[stateKey(string(e.Entry.GroupKey), e.Entry.Receiver)] = &e
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		return nil, err
	}
	return st, nil
}

func marshalMeshEntry(e *pb.MeshEntry) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := protodelim.MarshalTo(&buf, e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Options configures a new Log implementation.
type Options struct {
	SnapshotReader io.Reader
	SnapshotFile   string

	Retention time.Duration

	Logger  *slog.Logger
	Metrics prometheus.Registerer
}

func (o *Options) validate() error {
	if o.SnapshotFile != "" && o.SnapshotReader != nil {
		return errors.New("only one of SnapshotFile and SnapshotReader must be set")
	}

	if o.Metrics == nil {
		return errors.New("missing prometheus.Registerer")
	}

	return nil
}

// New creates a new notification log based on the provided options.
// The snapshot is loaded into the Log if it is set.
func New(o Options) (*Log, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	l := &Log{
		retention: o.Retention,
		logger:    promslog.NewNopLogger(),
		st:        state{},
		broadcast: func([]byte) {},
		metrics:   newMetrics(o.Metrics),
	}

	if o.Logger != nil {
		l.logger = o.Logger
	}

	if o.SnapshotFile != "" {
		// Resolve a candidate left behind by a crashed snapshot write
		// before opening the current snapshot file.
		if err := l.recoverSnapshotFile(o.SnapshotFile); err != nil {
			return nil, err
		}

		if r, err := os.Open(o.SnapshotFile); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
			l.logger.Debug("notification log snapshot file doesn't exist", "err", err)
		} else {
			err := l.loadSnapshotFile(r)
			_ = r.Close()
			if err != nil {
				return l, err
			}
		}
	}

	if o.SnapshotReader != nil {
		if err := l.loadSnapshot(o.SnapshotReader); err != nil {
			return l, err
		}
	}

	return l, nil
}

func (l *Log) now() time.Time {
	return time.Now()
}

// Maintenance garbage collects the notification log state at the given interval. If the snapshot
// file is set, a snapshot is written to it afterwards.
// Terminates on receiving from stopc.
// If not nil, the last argument is an override for what to do as part of the maintenance - for advanced usage.
func (l *Log) Maintenance(interval time.Duration, snapf string, stopc <-chan struct{}, override MaintenanceFunc) {
	if interval == 0 || stopc == nil {
		l.logger.Error("interval or stop signal are missing - not running maintenance")
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	var doMaintenance MaintenanceFunc
	doMaintenance = func() (int64, error) {
		if _, err := l.GC(); err != nil {
			return 0, err
		}
		if snapf == "" {
			return 0, nil
		}
		return l.writeSnapshotFile(snapf)
	}

	if override != nil {
		doMaintenance = override
	}

	runMaintenance := func(do func() (int64, error)) error {
		l.metrics.maintenanceTotal.Inc()
		start := l.now().UTC()
		l.logger.Debug("Running maintenance")
		size, err := do()
		l.metrics.snapshotSize.Set(float64(size))
		if err != nil {
			l.metrics.maintenanceErrorsTotal.Inc()
			return err
		}
		l.logger.Debug("Maintenance done", "duration", l.now().Sub(start), "size", size)
		return nil
	}

Loop:
	for {
		select {
		case <-stopc:
			break Loop
		case <-t.C:
			if err := runMaintenance(doMaintenance); err != nil {
				l.logger.Error("Running maintenance failed", "err", err)
			}
		}
	}

	// No need to run final maintenance if we don't want to snapshot.
	if snapf == "" {
		return
	}
	if err := runMaintenance(doMaintenance); err != nil {
		l.logger.Error("Creating shutdown snapshot failed", "err", err)
	}
}

func receiverKey(r *pb.Receiver) string {
	return fmt.Sprintf("%s/%s/%d", r.GroupName, r.Integration, r.Idx)
}

// stateKey returns a string key for a log entry consisting of the group key
// and receiver.
func stateKey(k string, r *pb.Receiver) string {
	return fmt.Sprintf("%s:%s", k, receiverKey(r))
}

func (l *Log) Log(r *pb.Receiver, gkey string, firingAlerts, resolvedAlerts, mutedAlerts []uint64, store *Store, expiry time.Duration) error {
	// Write all st with the same timestamp.
	now := l.now()
	key := stateKey(gkey, r)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	if prevle, ok := l.st[key]; ok {
		// Entry already exists, only overwrite if timestamp is newer.
		// This may happen with raciness or clock-drift across AM nodes.
		if prevle.Entry.Timestamp.AsTime().After(now) {
			return nil
		}
	}

	expiresAt := now.Add(l.retention)
	if expiry > 0 && l.retention > expiry {
		expiresAt = now.Add(expiry)
	}

	var receiverData map[string]*pb.ReceiverDataValue
	if store != nil {
		receiverData = store.data
	}

	e := &pb.MeshEntry{
		Entry: &pb.Entry{
			Receiver:       r,
			GroupKey:       []byte(gkey),
			Timestamp:      timestamppb.New(now),
			FiringAlerts:   firingAlerts,
			ResolvedAlerts: resolvedAlerts,
			MutedAlerts:    mutedAlerts,
			ReceiverData:   receiverData,
		},
		ExpiresAt: timestamppb.New(expiresAt),
	}

	b, err := marshalMeshEntry(e)
	if err != nil {
		return err
	}
	l.st.merge(e, l.now())
	l.broadcast(b)

	return nil
}

// GC implements the Log interface.
func (l *Log) GC() (int, error) {
	start := time.Now()
	defer func() { l.metrics.gcDuration.Observe(time.Since(start).Seconds()) }()

	now := l.now()
	var n int

	l.mtx.Lock()
	defer l.mtx.Unlock()

	for k, le := range l.st {
		if le.ExpiresAt.AsTime().IsZero() {
			return n, errors.New("unexpected zero expiration timestamp")
		}
		if !le.ExpiresAt.AsTime().After(now) {
			delete(l.st, k)
			n++
		}
	}

	return n, nil
}

// Query implements the Log interface.
func (l *Log) Query(params ...QueryParam) ([]*pb.Entry, error) {
	start := time.Now()
	l.metrics.queriesTotal.Inc()

	entries, err := func() ([]*pb.Entry, error) {
		q := &query{}
		for _, p := range params {
			if err := p(q); err != nil {
				return nil, err
			}
		}
		// TODO(fabxc): For now our only query mode is the most recent entry for a
		// receiver/group_key combination.
		if q.recv == nil || q.groupKey == "" {
			// TODO(fabxc): allow more complex queries in the future.
			// How to enable pagination?
			return nil, errors.New("no query parameters specified")
		}

		l.mtx.RLock()
		defer l.mtx.RUnlock()

		if le, ok := l.st[stateKey(q.groupKey, q.recv)]; ok {
			return []*pb.Entry{le.Entry}, nil
		}
		return nil, ErrNotFound
	}()
	if err != nil {
		l.metrics.queryErrorsTotal.Inc()
	}
	l.metrics.queryDuration.Observe(time.Since(start).Seconds())
	return entries, err
}

// loadSnapshot loads a snapshot generated by Snapshot() into the state.
func (l *Log) loadSnapshot(r io.Reader) error {
	st, err := decodeState(r)
	if err != nil {
		return err
	}

	l.mtx.Lock()
	l.st = st
	l.mtx.Unlock()

	return nil
}

// loadSnapshotFile loads a snapshot file into the state. Both the
// checksummed snapshot envelope and the legacy raw stream format are
// accepted.
func (l *Log) loadSnapshotFile(f *os.File) error {
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	payload, err := decodeSnapshotBlob(b)
	if err != nil {
		return err
	}
	return l.loadSnapshot(bytes.NewReader(payload))
}

// Snapshot implements the Log interface.
func (l *Log) Snapshot(w io.Writer) (int64, error) {
	start := time.Now()
	defer func() { l.metrics.snapshotDuration.Observe(time.Since(start).Seconds()) }()

	l.mtx.RLock()
	defer l.mtx.RUnlock()

	b, err := l.st.MarshalBinary()
	if err != nil {
		return 0, err
	}

	return io.Copy(w, bytes.NewReader(b))
}

// MarshalBinary serializes all contents of the notification log.
func (l *Log) MarshalBinary() ([]byte, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.st.MarshalBinary()
}

// Merge merges notification log state received from the cluster with the local state.
func (l *Log) Merge(b []byte) error {
	st, err := decodeState(bytes.NewReader(b))
	if err != nil {
		return err
	}
	l.mtx.Lock()
	defer l.mtx.Unlock()
	now := l.now()

	for _, e := range st {
		if merged := l.st.merge(e, now); merged && !cluster.OversizedMessage(b) {
			// If this is the first we've seen the message and it's
			// not oversized, gossip it to other nodes. We don't
			// propagate oversized messages because they're sent to
			// all nodes already.
			l.broadcast(b)
			l.metrics.propagatedMessagesTotal.Inc()
			l.logger.Debug("gossiping new entry", "entry", e)
		}
	}
	return nil
}

// SetBroadcast sets a broadcast callback that will be invoked with serialized state
// on updates.
func (l *Log) SetBroadcast(f func([]byte)) {
	l.mtx.Lock()
	l.broadcast = f
	l.mtx.Unlock()
}

func snapshotCandidatePath(filename string) string {
	return filename + snapshotCandidateSuffix
}

// encodeSnapshot wraps the raw state payload in the recoverable snapshot
// envelope: a magic prefix and a trailing CRC-32 checksum of the payload.
func encodeSnapshot(payload []byte) []byte {
	b := make([]byte, 0, len(snapshotMagicV1)+len(payload)+crc32.Size)
	b = append(b, snapshotMagicV1...)
	b = append(b, payload...)

	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
	return append(b, checksum[:]...)
}

// decodeSnapshotBlob validates the snapshot envelope and returns its raw
// payload. Bytes without the envelope magic are returned unchanged so that
// snapshots written by older Alertmanager versions keep loading.
func decodeSnapshotBlob(b []byte) ([]byte, error) {
	if !bytes.HasPrefix(b, snapshotMagicV1) {
		return b, nil
	}
	if len(b) < len(snapshotMagicV1)+crc32.Size {
		return nil, ErrSnapshotCorrupt
	}
	split := len(b) - crc32.Size
	payload := b[len(snapshotMagicV1):split]
	if binary.BigEndian.Uint32(b[split:]) != crc32.ChecksumIEEE(payload) {
		return nil, ErrSnapshotCorrupt
	}
	return payload, nil
}

// verifySnapshotFile reads a snapshot candidate, validates its checksum and
// fully decodes its payload. Only a file for which both checks succeed may
// replace the current snapshot generation.
func verifySnapshotFile(filename string) (state, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	payload, err := decodeSnapshotBlob(b)
	if err != nil {
		return nil, err
	}
	st, err := decodeState(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	return st, nil
}

// writeSnapshotFile commits a new snapshot generation to filename. Expired
// entries are pruned and the encoded candidate is synced, checksummed and
// fully decoded from disk before it atomically replaces the current file.
// If encoding, syncing or verification fails, the candidate is discarded and
// the previous snapshot file is left untouched.
func (l *Log) writeSnapshotFile(filename string) (int64, error) {
	start := l.now()
	defer func() { l.metrics.snapshotDuration.Observe(l.now().Sub(start).Seconds()) }()

	now := l.now()

	l.mtx.RLock()
	payload, err := l.st.marshalBinaryPruned(now)
	l.mtx.RUnlock()
	if err != nil {
		return 0, err
	}

	candidate := snapshotCandidatePath(filename)

	// abort discards the candidate. The committed snapshot generation is
	// never touched on failure.
	abort := func(cause error) (int64, error) {
		if err := os.Remove(candidate); err != nil && !os.IsNotExist(err) {
			l.logger.Debug("failed to remove snapshot candidate", "file", candidate, "err", err)
		}
		return 0, cause
	}

	f, err := os.OpenFile(candidate, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return abort(err)
	}
	if _, err := f.Write(encodeSnapshot(payload)); err != nil {
		_ = f.Close()
		return abort(err)
	}
	// Flush the candidate to stable storage before reading it back.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return abort(err)
	}
	if err := f.Close(); err != nil {
		return abort(err)
	}

	// Promote the candidate only once its checksum verifies and the whole
	// payload decodes.
	if _, err := verifySnapshotFile(candidate); err != nil {
		return abort(err)
	}

	if err := os.Rename(candidate, filename); err != nil {
		return abort(err)
	}
	syncSnapshotDir(l.logger, filepath.Dir(filename))

	generation := l.generation.Add(1)
	l.logger.Debug("notification log snapshot committed", "generation", generation, "size", len(payload))

	return int64(len(payload)), nil
}

// recoverSnapshotFile resolves a snapshot generation left behind by a
// previous process. A complete, valid candidate is promoted over the current
// snapshot; an incomplete or corrupt candidate is removed while the last
// valid snapshot is preserved. Stale temporary files left by older
// Alertmanager versions ("<snapshot>.<random>") are cleaned up as well.
func (l *Log) recoverSnapshotFile(filename string) error {
	candidate := snapshotCandidatePath(filename)

	if _, err := os.Stat(candidate); err == nil {
		if _, verr := verifySnapshotFile(candidate); verr != nil {
			l.logger.Warn("discarding incomplete notification log snapshot candidate", "file", candidate, "err", verr)
			if err := os.Remove(candidate); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("removing corrupt snapshot candidate: %w", err)
			}
		} else if err := os.Rename(candidate, filename); err != nil {
			return fmt.Errorf("recovering snapshot candidate: %w", err)
		} else {
			l.logger.Info("recovered notification log snapshot from completed candidate", "file", filename)
			syncSnapshotDir(l.logger, filepath.Dir(filename))
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspecting snapshot candidate: %w", err)
	}

	l.removeStaleSnapshotFiles(filename, candidate)
	return nil
}

// removeStaleSnapshotFiles removes temporary snapshot files that are neither
// the committed snapshot nor its candidate. Only files using the legacy
// random temp naming ("<snapshot>.<hex>") are removed, operator-managed files
// are left untouched.
func (l *Log) removeStaleSnapshotFiles(filename, candidate string) {
	prefix := filename + "."
	matches, err := filepath.Glob(prefix + "*")
	if err != nil {
		l.logger.Debug("listing stale snapshot files failed", "err", err)
		return
	}
	for _, m := range matches {
		if m == candidate {
			continue
		}
		suffix, ok := strings.CutPrefix(m, prefix)
		if !ok || !isLegacySnapshotTempSuffix(suffix) {
			continue
		}
		if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
			l.logger.Debug("removing stale snapshot file failed", "file", m, "err", err)
			continue
		}
		l.logger.Debug("removed stale snapshot file", "file", m)
	}
}

// isLegacySnapshotTempSuffix reports whether name matches the random hex
// suffix of temporary snapshot files written by older Alertmanager versions.
func isLegacySnapshotTempSuffix(suffix string) bool {
	if len(suffix) == 0 || len(suffix) > 16 {
		return false
	}
	for _, r := range suffix {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// syncSnapshotDir flushes the directory entry for the snapshot rename so the
// new generation survives a crash or power loss. Failures are non-fatal and
// do not invalidate the already committed snapshot.
func syncSnapshotDir(logger *slog.Logger, dir string) {
	f, err := os.Open(dir)
	if err != nil {
		logger.Debug("opening snapshot directory for sync failed", "dir", dir, "err", err)
		return
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		logger.Debug("syncing snapshot directory failed", "dir", dir, "err", err)
	}
}
