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

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout       int
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, confState, _ := c.Storage.InitialState()
	raftLog := newLog(c.Storage)
	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}
	peers := make(map[uint64]*Progress)
	for _, id := range c.peers {
		peers[id] = &Progress{
			Match: 0,
			Next:  raftLog.LastIndex() + 1,
		}
	}
	for _, id := range confState.Nodes {
		if _, ok := peers[id]; !ok {
			peers[id] = &Progress{
				Match: 0,
				Next:  raftLog.LastIndex() + 1,
			}
		}
	}
	return &Raft{
		id:                    c.ID,
		Term:                  hardState.Term,
		Vote:                  hardState.Vote,
		RaftLog:               raftLog,
		Prs:                   peers,
		Lead:                  None,
		State:                 StateFollower,
		votes:                 make(map[uint64]bool),
		msgs:                  make([]pb.Message, 0),
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:      0,
		electionElapsed:       0,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	prevIndex := r.Prs[to].Next - 1
	prevTerm, _ := r.RaftLog.Term(prevIndex)
	var entries []*pb.Entry
	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		offset := r.RaftLog.entries[0].Index
		entries = append(entries, &r.RaftLog.entries[i-offset])
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed < r.heartbeatTimeout {
			return
		}
		r.heartbeatElapsed = 0
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed < r.randomElectionTimeout {
			return
		}
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.State = StateCandidate
	r.msgs = make([]pb.Message, 0)
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	noopEntry := pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)
	if pr, ok := r.Prs[r.id]; ok {
		pr.Match = noopEntry.Index
		pr.Next = pr.Match + 1
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = noopEntry.Index
	}
}

func (r *Raft) campaign() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if _, ok := r.Prs[r.id]; !ok {
		return nil
	}
	if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgAppend {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		if r.State == StateCandidate {
			r.campaign()
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		if r.State == StateCandidate {
			r.campaign()
		}
	case pb.MessageType_MsgHeartbeat, pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		if r.Term >= m.Term {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleMsgBeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleLeaderRequestVote(m)
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleMsgAppendResponse(m)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.Vote = m.From
			reject = false
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = m.Reject == false
	grants, rejects := 0, 0
	quorum := len(r.Prs)/2 + 1
	for _, v := range r.votes {
		if v {
			grants++
		} else {
			rejects++
		}
	}
	if grants >= quorum {
		r.becomeLeader()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	} else if rejects >= quorum {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	for peer := range r.Prs {
		if peer != r.id {
			if r.Prs[peer].Match > 0 && r.Prs[peer].Match < r.RaftLog.LastIndex() {
				r.sendAppend(peer)
			} else {
				r.sendHeartbeat(peer)
			}
		}
	}
}

func (r *Raft) handleLeaderRequestVote(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	for _, e := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			Term:  r.Term,
			Index: r.RaftLog.LastIndex() + 1,
			Data:  e.Data,
		})
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		quorum := len(r.Prs)/2 + 1
		oldCommit := r.RaftLog.committed
		for i := r.RaftLog.LastIndex(); i >= 1; i-- {
			grants := 0
			for _, pr := range r.Prs {
				if pr.Match >= i {
					grants++
				}
			}
			if grants >= quorum {
				term, _ := r.RaftLog.Term(i)
				if term == r.Term {
					r.RaftLog.committed = i
				}
				break
			}
		}
		if r.RaftLog.committed > oldCommit {
			for peer := range r.Prs {
				if peer != r.id {
					r.sendAppend(peer)
				}
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.electionElapsed = 0
	r.Lead = m.From
	term, _ := r.RaftLog.Term(m.Index)
	if term != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}
	offset := r.RaftLog.entries[0].Index
	for i, e := range m.Entries {
		if e.Index-offset >= uint64(len(r.RaftLog.entries)) {
			for _, ne := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *ne)
			}
			break
		}
		ee := r.RaftLog.entries[e.Index-offset]
		if ee.Term != e.Term {
			r.RaftLog.stabled = min(r.RaftLog.stabled, e.Index-1)
			r.RaftLog.entries = r.RaftLog.entries[:e.Index-offset]
			for _, ne := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *ne)
			}
			break
		}
	}
	lastNewIndex := m.Index + uint64(len(m.Entries))
	r.RaftLog.committed = min(m.Commit, lastNewIndex)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.electionElapsed = 0
	r.Lead = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		LogTerm: r.Term,
		Term:    r.RaftLog.committed,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
