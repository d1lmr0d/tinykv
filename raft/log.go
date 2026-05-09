// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		firstIndex = 1
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		lastIndex = firstIndex - 1
	}
	term, _ := storage.Term(firstIndex - 1)

	entries := make([]pb.Entry, 0)
	entries = append(entries, pb.Entry{Term: term, Index: firstIndex - 1})
	if lastIndex >= firstIndex {
		if v, err := storage.Entries(firstIndex, lastIndex+1); err == nil {
			entries = append(entries, v...)
		}
	}

	hardState, _, _ := storage.InitialState()
	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) <= 1 {
		return nil
	}
	entries := make([]pb.Entry, len(l.entries)-1)
	copy(entries, l.entries[1:])
	return entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) <= 1 {
		return nil
	}
	offset := l.entries[0].Index
	unstable := int(l.stabled - offset + 1)
	if unstable > len(l.entries) {
		return nil
	}
	entries := make([]pb.Entry, len(l.entries)-unstable)
	copy(entries, l.entries[unstable:])
	return entries
}

// nextEntries returns all the committed but not applied entries
func (l *RaftLog) nextEntries() (ents []pb.Entry) {
	offset := l.entries[0].Index
	applied := int(l.applied - offset + 1)
	committed := int(l.committed - offset + 1)
	if committed > len(l.entries) {
		committed = len(l.entries)
	}
	entries := make([]pb.Entry, committed-applied)
	copy(entries, l.entries[applied:committed])
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	offset := l.entries[0].Index
	if i < offset {
		return l.storage.Term(i) // Snapshot
	}
	idx := int(i - offset)
	if idx >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[idx].Term, nil
}
