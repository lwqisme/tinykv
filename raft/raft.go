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
	"crypto/rand"
	"errors"
	"math/big"
	"sort"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
	// Match 代表leader视角的对应的follower的进度
	Match uint64
	// Next follower的进度+1
	Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// 表示 peers 进度到哪里，表示的是最新的，无需关注 committed/applied
	// 需要包含自己！
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	voteRejects map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// peers 包括 the IDs of all nodes (including self) in the raft cluster.
	peers []uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// electionTimeout 的基础上加上随机的数。
	electionTimeoutPlus int

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
	// Your Code Here (2A).
	var err error
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		return nil
	}

	raftLog := newLog(c.Storage)
	raftLog.stabled, err = raftLog.storage.LastIndex()
	if err != nil {
		return nil
	}
	raftLog.committed = hardState.Commit

	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = &Progress{Next: raftLog.LastIndex() + 1, Match: raftLog.LastIndex()}
	}

	res := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		peers:            c.peers,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            make(map[uint64]bool),
		voteRejects:      make(map[uint64]bool),
	}

	res.Prs = prs
	return res
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// 这里是逐个Progress去遍历
	toProgress := r.Prs[to]

	commitIndex := r.RaftLog.committed
	preIndex := toProgress.Next - 1 // 获取要发送的 log 的前置 term 以及 index
	preTerm := uint64(0)
	if preIndex != 0 { // 当 preIndex 为0，对应 dummy ，使用 Entries 方法取不到，默认 term 0
		ents := r.RaftLog.Entries(preIndex, preIndex+1)
		if len(ents) != 1 {
			return false
		}
		preTerm = ents[0].Term
	}

	entries := r.RaftLog.Entries(toProgress.Next, r.RaftLog.LastIndex()+1)
	entriesPointers := make([]*pb.Entry, 0)
	for i := 0; i < len(entries); i++ {
		entriesPointers = append(entriesPointers, &entries[i])
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   preIndex, // 要发送的 log 的前置 index
		LogTerm: preTerm,  // 要发送的 log 的前置 term
		Commit:  commitIndex,
		Entries: entriesPointers,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// logIndex := r.RaftLog.LastIndex()
	// logTerm, err := r.RaftLog.Term(logIndex)
	// if err != nil {
	// 	return
	// }
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		// LogTerm: logTerm, // 在 Heartbeat 中， LogTerm 发送的是消息发送者的最新条目的 term
		// Index:   logIndex,
		Commit: r.RaftLog.committed,
	})
}

// sendElection leader send election message to other nodes.
func (r *Raft) sendElection(to uint64) error {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		return err
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
	})
	return nil
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	if r.heartbeatElapsed == r.heartbeatTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		r.heartbeatElapsed = 0
	}
	r.electionElapsed++
	if r.electionElapsed == r.electionTimeout+r.electionTimeoutPlus {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		r.electionElapsed = 0
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Prs = make(map[uint64]*Progress, 0)
	r.Term = term
	r.Lead = lead
	r.votes = make(map[uint64]bool, 0)
	r.voteRejects = make(map[uint64]bool, 0)
	r.Vote = 0
	r.electionElapsed = 0
	randPlus, _ := rand.Int(rand.Reader, big.NewInt(int64(r.electionTimeout)))
	r.electionTimeoutPlus = int(randPlus.Int64())
}

// becomeCandidate transform this peer's state to candidate
// Term will increase in this function
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Prs = make(map[uint64]*Progress, 0)
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.Lead = 0
	r.electionElapsed = 0
	randPlus, _ := rand.Int(rand.Reader, big.NewInt(int64(r.electionTimeout)))
	r.electionTimeoutPlus = int(randPlus.Int64())
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.State = StateLeader
	r.Vote = 0
	r.votes = make(map[uint64]bool, 0)
	r.voteRejects = make(map[uint64]bool, 0)
	r.Lead = 0
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	// 是否需要处理以前的progress？这里直接重置所有的progress
	// 这里需要将Prs初始化为自己的lastIndex
	r.Prs = make(map[uint64]*Progress, 0)
	li := r.RaftLog.LastIndex()
	for _, p := range r.peers {
		r.Prs[p] = &Progress{
			Match: li,
			Next:  li + 1,
		}
	}

	// 变成了leader之后无需马上发送消息，而是将空消息写入本地之后，其他的方法去同步到其他的peers
	// 但是如果peers只有一个的话，不会有其他的机器返回消息给它，所以需要自己设置commited变量
	nilEntries := []eraftpb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}}
	r.RaftLog.AppendEntries(nilEntries, r.Term)
	if len(r.peers) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		// r.RaftLog.applied = r.RaftLog.committed
	}
	r.changePrs(addPrsMode, r.id, uint64(len(nilEntries)))
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// TODO：问题比较大，有的情况可能不需要
	if m.Term > r.Term && r.State != StateFollower {
		// 其实也不清楚是否所有情况都需要马上转换为 follower
		lead := uint64(0)
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}
	if r.State == StateLeader {
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			// 获取各个peer的id，然后逐个发送heartbeat给它们。
			r.handleBeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleResponseVote(m)
	case pb.MessageType_MsgPropose:
		// 重定向到leader
		if r.State == StateFollower {
			r.redirctPropose(m)
		} else if r.State == StateLeader {
			r.handlePropose(m)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
	// 这个顺序有可能要提前的。
	if m.Term > r.Term && r.State == StateFollower {
		r.Term = m.Term
		// r.Vote = 0
	}
	// 善变的 follower
	if m.Term >= r.Term && r.State == StateFollower {
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend {
			r.Lead = m.From
		}
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// 此方法是 follower 接收到Append请求的，只需要将请求中的entries写入到日志即可。
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 我也不知道，term 相等的 leader 之间互相传信息会发生什么
	if m.Term < r.Term && r.State == StateLeader { // ignore
		return
	}
	reject := false
	// 如果两者在同一个Term，那么被Append的作为follower
	// TODO：如果这个后来的也恰好接收到一个Append请求怎么办呢？
	if m.Term == r.Term && r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
		return
	}

	lastNewIndex := m.Index

	if m.LogTerm != 0 {
		// 检查前置log是否一致，不一致则拒绝。
		ents := r.RaftLog.Entries(m.Index, m.Index+1)
		if (len(ents) == 1 && ents[0].Term != m.LogTerm) || len(ents) != 1 {
			reject = true
		}
	}
	if !reject {
		// if len(m.Entries) > 0 && m.Entries[0].Index <= r.RaftLog.LastIndex() {
		// 	// 如果发现有冲突，并且term更高，那么把冲突 index (不包括index) 以后的记录都删除。
		// 	del := r.RaftLog.DealConflict(m.Entries[0].Term, m.Entries[0].Index)
		// 	if !del && m.Entries[0].Index < r.RaftLog.LastIndex() { // 没有冲突

		// 	}
		// } else {
		// 	// TODO：不清楚当消息的commit低于本地commit会发生什么
		// 	if m.Commit > r.RaftLog.committed {
		// 		r.RaftLog.committed = m.Commit
		// 		r.RaftLog.applied = r.RaftLog.committed
		// 	}
		// }

		ents := []pb.Entry{}
		var del bool
		for _, ent := range m.Entries {
			lastNewIndex = max(lastNewIndex, ent.Index)
			if !del && ent.Index <= r.RaftLog.LastIndex() { // 只需要处理一次冲突，后续不需要了
				del = r.RaftLog.DealConflict(ent.Term, ent.Index)
				if !del {
					continue
				}
			}
			ents = append(ents, *ent)
		}
		err := r.RaftLog.AppendEntries(ents, r.Term)
		if err != nil {
			return
		}

		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, lastNewIndex)
			// r.RaftLog.applied = r.RaftLog.committed
		}

		r.changePrs(addPrsMode, r.id, uint64(len(ents)))
	}

	// 不需要假装了，因为在单测调用了MemoryStorage的。
	// 假装持久化
	// r.RaftLog.stabled += uint64(len(m.Entries))

	r.msgs = append(r.msgs, pb.Message{
		MsgType: eraftpb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	})
}

// handleAppendResponse leader接受AppendEndtries的结果
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject { // 如果被拒绝了，就将 nextindex 和 matchindex 都减一重试
		r.changePrs(minusPrsMode, m.From, 1)
		r.sendAppend(m.From)
		return
	}

	// 需要对 MessageType_MsgAppendResponse 进行计数，

	// TODO：此前应该需要判断一下 m.Index 是否合理，和本地是否相同
	// 此时到 Prs 中更新 peers 的进度。
	index := m.Index
	r.changePrs(setPrsMode, m.From, index)

	// 只有 index 对应条目在当前的 term 中时，才允许更新committed
	ents := r.RaftLog.Entries(index, index+1)
	if len(ents) == 1 && ents[0].Term == r.Term {
		// 使用index剪枝
		if index > r.RaftLog.committed {
			// 统计超过半数的 index，然后更新 leader 的 commited 参数
			arr := []uint64{r.RaftLog.LastIndex()}
			for id, progress := range r.Prs {
				if id != r.id {
					arr = append(arr, progress.Match)
				}
			}
			sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
			majority := arr[(len(arr)-1)/2]
			if majority > r.RaftLog.committed {
				r.RaftLog.committed = majority

				// r.RaftLog.applied = r.RaftLog.committed

				// 一旦 commited 更新了，
				// 需要将携带Commit信息的MsgAppend消息发送出去
				r.bcastAppend()
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reject := false
	// leader不做响应
	if r.State == StateLeader {
		// TODO：这里应该需要判断一下来源的
		return
	}

	// 在接收到term高于自己的heartbeat之后，candidate需要将自己转换为普通的follower
	// 这里的Term使用的是node的Term而不是LogTerm
	if m.Term > r.Term {
		if r.State == StateCandidate { // candidate
			// 回滚为follower，并且更新committed index from the one in this heartbeat
			r.becomeFollower(m.Term, m.From)
			r.RaftLog.committed = m.Commit
		} else { // follower
			if m.Term > r.Term {
				r.Lead = m.From
			}
		}
	} else if m.Term < r.Term { // 当 m.Term <= r.Term 时，代表 heartbeat 是过时的或者正在进行时
		return
	}

	// 判断来源的 heartbeat 是否up-to-date
	// TODO：TestLeaderBcastBeat2AA 要求 消息的 LogTerm 以及 Index 都为0
	// TODO：和 TestCommitWithHeartbeat2AB 冲突了
	// TODO：和 TestHeartbeatUpdateCommit2AB 冲突了
	// switch r.isUpToDate(m.LogTerm, m.Index) {
	// case newer, older:
	// 	reject = true
	// case equal:
	// 	reject = false
	// }
	// if m.Commit > r.RaftLog.committed {
	// 	r.RaftLog.committed = m.Commit
	// }

	r.electionElapsed = 0 // 然后需要将election倒计时重置

	r.Term = m.Term // 设置term

	// 发送heartbeat的回复
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	})
}

const (
	newer int = iota
	equal
	older
)

// isUpToDate 判断传入的 term, index 是否最新
func (r *Raft) isUpToDate(logTerm, logIndex uint64) int {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		return -1
	}

	if logIndex == 0 {
		if lastIndex == 0 {
			return equal
		} else if logTerm <= lastTerm {
			return older
		} else {
			return newer
		}
	}

	ents := r.RaftLog.Entries(logIndex, logIndex+1)

	if (len(ents) == 1 && (logTerm < ents[0].Term || (logTerm == ents[0].Term && logIndex < r.RaftLog.LastIndex()))) ||
		(len(ents) == 0 && (logTerm < lastTerm)) {
		// 来源 heartbeat 不是 up-to-date
		return older
	} else if len(ents) == 1 && logIndex == r.RaftLog.LastIndex() && logTerm == ents[0].Term {
		// 传入的 term, index 与本地最新的是相同的
		return equal
	} else {
		return newer
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleRequestVote handle the MessageType_MsgRequestVote message to vote for a candidate node from RPC request.
func (r *Raft) handleRequestVote(m pb.Message) error {
	// 接收到Vote请求之后，本node需要给发送方投票，
	// 3种情况
	// 1. 本身是follower，最近没有人申请投票，所以直接投。
	// 2. 本身是follower，最近有人申请投票，已经投给别人了，所以这次投票不投了，但是需要回复。
	// 3. 本身是candidate或者leader，不投票，但是需要回复，或者反过来跟上进度。
	// 这就导致了我们需要在结束了投票之后需要将 r 中的 Vote 属性重置为0

	// 当node为candidate或者leader时，
	// 接收到的msg的Term比自己的要低，则拒绝投票，Reject为true。
	// 接收到的msg的Term比自己的要高，则自己变身为follower，跟随那个leader。
	reject := false
	if r.State == StateCandidate || r.State == StateLeader {
		reject = true
		if m.Term < r.Term {

		} else if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			// 还需要同步log的进度
			// TODO：是否需要 reject 呢？
		}
	} else {
		// lastTerm, err := r.RaftLog.Term(lastIndex)
		// if err != nil {
		// 	return err
		// }

		// upToDate 表示被投票的 candidate 是否已经是最新
		// upToDate := false

		// 只有当 Term 相同的时候才需要进行 up-to-date 检查
		// 如果传入 Term 小于本地，说明是过时，后面会进行 reject
		// 如果传入的 Term 大于本地，说明开始了新一轮的选举
		// 对前置 term, index 进行 up-to-date 检查
		// if m.LogTerm != 0 && m.Term == r.Term {
		// TODO：这里是否需要去掉 LogTerm 判空，默认LogTerm是存在？
		// TODO：我觉得这会出问题，因为有的其实并没有传LogTerm。
		// if m.LogTerm != 0 {
		// 当 LogTerm 小于本地 Term ，说明 candidate 不是最新
		// 当 LogTerm 与本地 Term 相等，但是消息的 Index 小于本地的最大 Index，说明 candidate 不是最新
		switch r.isUpToDate(m.LogTerm, m.Index) {
		case older:
			reject = true
		default:
			reject = false
		}
		// }

		// follower接收到。需要判断别人的term或者日志index是否大于我本地的，
		// 如果是，那么则同意投票；否则不同意投票
		// TODO: 我不清楚这里的 index 对比是否可以去掉了？因为上面的isUpToDate已经对比过了
		// lastIndex := r.RaftLog.LastIndex()
		// if !reject && (m.Term > r.Term || (m.Term == r.Term && m.Index >= lastIndex)) {
		if !reject && (m.Term >= r.Term) {
			// 按照term去进行划分。如果follower的term被更新为更高的值，那么 r.Vote 将被重置。
			// ↑但是我把 step 方法中的 becomeFollower 去掉了，所以没有重置 r.Vote
			if m.Term > r.Term {
				r.Vote = 0
			}
			// 最近没有投过其他的。
			// 这个Vote重置是在接收到了正确的heartbeat之后重置的。
			if r.Vote == 0 {
				reject = false
				r.Vote = m.From
				r.electionElapsed = 0
			} else if r.Vote == m.From {
				reject = false
				r.electionElapsed = 0
			} else {
				reject = true
			}
		} else {
			reject = true
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
	return nil
}

// handleResponseVote
func (r *Raft) handleResponseVote(m pb.Message) error {
	if r.State == StateCandidate {
		// 这块抽象出来可以用来处理Hup
		if !m.Reject {
			r.votes[m.From] = true
		} else {
			r.voteRejects[m.From] = true
		}

		if len(r.voteRejects) > len(r.peers)/2 {
			r.becomeFollower(r.Term, m.From)
		}
		if len(r.votes) > len(r.peers)/2 {
			r.becomeLeader()
			r.bcastAppend()
		}
		// TODO：这块抽象出来可以用来处理Hup
	}
	return nil
}

// handleHup handle local MessageType_MsgHup message to dispatch election message to peers.
// the node which run out of election timeout will get into this method.
func (r *Raft) handleHup(m pb.Message) {
	// leader 不需要处理 election timeout
	if r.State == StateLeader {
		return
	}
	// 当进入此函数，代表election timeout已经结束，进入candidate状态
	r.becomeCandidate()

	if len(r.peers) == 1 {
		r.becomeLeader()
		r.bcastAppend()
		return
	}

	// 获取各个peer的id，然后逐个发送election信息给它们。
	// 返回值我们会在Step函数里进行进一步处理
	for _, p := range r.peers {
		if r.id != p {
			r.sendElection(p)
		}
	}
}

// handleBeat handle local MessageType_MsgBeat message to dispatch heartbeat message to peers.
func (r *Raft) handleBeat(m pb.Message) {
	for _, p := range r.peers {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

// redirctPropose follower接收到Propose信息之后稍作包装，并重定向到leader
func (r *Raft) redirctPropose(m pb.Message) error {
	m.From = r.id // 重新设置sender
	m.To = r.Lead // 重新设置目标

	sto := r.RaftLog.storage

	var err error
	lastIndex := r.RaftLog.LastIndex()
	m.Term, err = sto.Term(lastIndex)
	if err != nil {
		return err
	}

	r.msgs = append(r.msgs, m)

	return nil
}

// handlePropose the leader append a new entry to Log from RPC request.
func (r *Raft) handlePropose(m pb.Message) error {
	// 走到此函数时，本node应该为leader状态
	if len(m.Entries) != 0 {
		ents := []pb.Entry{}
		li := r.RaftLog.LastIndex()
		for i, ent := range m.Entries {
			// 防止上游发癫传空
			ents = append(ents, *ent)
			if ents[i].Term == 0 {
				ents[i].Term = r.Term
			}
			if ents[i].Index == 0 {
				li++
				ents[i].Index = li
			}
		}
		err := r.RaftLog.AppendEntries(ents, r.Term)
		if err != nil {
			return err
		}
		r.changePrs(addPrsMode, r.id, uint64(len(ents)))

		r.bcastAppend()

		// 如果集群只有一台机器，那就必须要直接commit
		if len(r.peers) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()

			// r.RaftLog.applied = r.RaftLog.committed
		}
	}
	return nil
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) error {
	// TODO：接收到对方的返回说明对方的状态是正常的。

	if m.Index != r.RaftLog.LastIndex() {
		r.changePrs(setPrsMode, m.From, m.Index)
		r.sendAppend(m.From)
	}
	// 如果对方 reject ，说明对方的不是最新的。需要重新 append
	// if m.Reject {
	// 	r.sendAppend(m.From)
	// }
	return nil
}

// bcastAppend
func (r *Raft) bcastAppend() {
	for _, p := range r.peers {
		if r.id != p {
			r.sendAppend(p)
		}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

const (
	setPrsMode int = iota
	addPrsMode
	minusPrsMode
)

func (r *Raft) changePrs(mode int, id, val uint64) {
	if r.State != StateLeader {
		return
	}
	if mode == setPrsMode {
		r.Prs[id].Match = val
	} else if mode == addPrsMode {
		r.Prs[id].Match += val
	} else if mode == minusPrsMode {
		r.Prs[id].Match -= val
	}
	r.Prs[id].Next = r.Prs[id].Match + 1
}
