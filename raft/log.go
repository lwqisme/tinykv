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
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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
	// stabled 是本地持久化
	stabled uint64

	// all entries that have not yet compact.
	// 不包含dummy，因为 storage 的获取 entries 的接口访问到 dummy 条目会报错。
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 将 storage 中全部的日志写入到 log 中
	var fi, li uint64
	var err error
	allEntries := []pb.Entry{}
	if fi, err = storage.FirstIndex(); err == nil {
		if li, err = storage.LastIndex(); err == nil {
			// fi-1 的目的是获取到全部的条目，不能包括 dummy，否则会报错
			allEntries, err = storage.Entries(fi, li+1)
		}
	}
	if err != nil {
		return nil
	}

	return &RaftLog{
		storage: storage,
		// committed: li,
		// applied:   li,
		// stabled:   li,
		entries: allEntries,
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
	// Your Code Here (2A).
	// 以stable作为分割
	// if l.stabled > l.storedLastIndex() {
	// 	return nil
	// }
	// inStore, err := l.storage.Entries(1, l.stabled+1)
	// if err != nil {
	// 	return nil
	// }
	// inLog := l.unstableEntries()

	// inStore = append(inStore, inLog...)

	// return inStore

	// 假设 l.entries 是全部条目(不包含dummy)，返回全部
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.Entries(l.stabled+1, l.LastIndex()+1)
}

// Entries return entries[lo,hi)
// lo 一般不能为 0 ，因为 RaftLog 中的 entries 不包含 dummy 条目
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	// 首先保证 lo 和 hi 的逻辑关系要成立。
	zeroReturn := make([]pb.Entry, 0)
	if lo >= hi {
		return zeroReturn
	}
	// 使用lastIndex限制了获取的范围，如果超出直接不返回
	// 下方的logEntries中做了检验范围，只是为了保证不会发生数组越界
	if hi-1 > l.LastIndex() {
		return zeroReturn
	}

	// 假设 l.entries 是全部的条目，则不需要分开到 storage 中获取
	// if lo <= l.stabled || len(l.entries) == 0 {
	// 	if hi-1 <= l.stabled || len(l.entries) == 0 {
	// 		sli, err := l.storage.LastIndex() // 帮 l.storage.Entries 检查边界
	// 		if err != nil || hi > sli+1 {
	// 			return zeroReturn
	// 		}
	// 		res, err := l.storage.Entries(lo, hi)
	// 		if err != nil {
	// 			return zeroReturn
	// 		}
	// 		return res
	// 	} else {
	// 		sli, err := l.storage.LastIndex() // 帮 l.storage.Entries 检查边界
	// 		if err != nil || l.stabled > sli {
	// 			return zeroReturn
	// 		}
	// 		res, err := l.storage.Entries(lo, l.stabled+1)
	// 		if err != nil {
	// 			return zeroReturn
	// 		}
	// 		res = append(res, l.logEntries(l.stabled+1, hi)...)
	// 		return res
	// 	}
	// } else {
	// 	return l.logEntries(lo, hi)
	// }

	return l.logEntries(lo, hi)
}

func (l *RaftLog) startIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

// logEntries 内部函数，用于获取 RaftLog 中存储的entries
// 传入的 lo 和 hi 必须在 RaftLog 的entries的范围内，避免越界
func (l *RaftLog) logEntries(lo, hi uint64) []pb.Entry {
	start := l.startIndex()
	end := l.LastIndex()
	if lo == 0 {
		lo = start
	}
	if hi == 0 {
		hi = start + uint64(len(l.entries))
	}
	if lo < start || hi-1 < start || lo > end || hi-1 > end || lo >= hi ||
		hi-start > uint64(len(l.entries)) {
		return nil
	}

	return l.entries[lo-start : hi-start]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.Entries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 { // 因为内存和 storage 不一定同步，所以不再到 storage 中取
		// 还是要到 storage 中取，因为 tmd 它初始化的 index 是 5
		return l.storedLastIndex()
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 在存储中确定了的部分，都到存储中去获取。
	if i <= l.storedLastIndex() {
		term, err := l.storage.Term(i)
		if err != nil {
			return 0, err
		}
		return term, nil
	}

	// 剩余未确定的部分，在本地 RaftLog 中获取。
	if i > l.LastIndex() {
		return 0, errors.New("i pass to Term function out of index")
	} else {
		return l.entries[i-l.startIndex()].Term, nil
	}
}

// AppendEntries 外部的Raft尽量不要直接与该函数进行交互，
// 调用 r.appendEntries 在外部确定了Term之后再进入此处
func (l *RaftLog) AppendEntries(entries []pb.Entry, term uint64) error {
	if len(entries) == 0 {
		return nil
	}

	// 这里难道 当本地的 entries 为空时，可以直接添加进去？
	// 因为有可能冲突的条都已经持久化了，但是全都冲突了之后去获取 store 的，会获取到冲突的部分的。所以不用管，直接插入。
	if len(l.entries) == 0 {
		l.entries = append(l.entries, entries...)
	} else if l.LastIndex()+1 == entries[0].Index {
		l.entries = append(l.entries, entries...)
	} else {
		return fmt.Errorf("appended log entries are not continuous [last: %d, append at: %d]",
			l.LastIndex(), entries[0].Index)
	}

	return nil
}

func (l *RaftLog) storedLastIndex() uint64 {
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return lastIndex
}

func (l *RaftLog) storedLastTerm() uint64 {
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return lastIndex
}

// DealConflict 一旦冲突，将会直接将过去的 stabled 和 lastIndex 往前推移
// 造成一部分记录被删除的现象。
// note: 被删除的日志包括 index 本身
func (l *RaftLog) DealConflict(term, index uint64) (isDel bool) {
	if index == 0 {
		// 避免越界，这里应该是直接报错，但是我没有写 error 返回值
		return false
	}

	si := l.startIndex()

	// 获取index对应的
	ents := l.Entries(index, index+1)
	// if len(ents) == 1 && ents[0].Term < term {
	// 该函数的执行方是 Follower，所以一切以 leader 为准，只要和 leader 不相同的都删除。
	if len(ents) == 1 && ents[0].Term != term {
		// 直接删除这一条index往后的所有条
		if index <= l.stabled {
			l.stabled = index - 1
			// l.lastIndex = l.stabled
			l.entries = l.entries[:index-si]
		} else if index <= l.LastIndex() {
			// l.lastIndex = index - 1
			l.entries = l.entries[:index-si]
		} else {
			return false
		}
		return true
	} else if len(ents) != 1 {
		return false
	}
	return false
}

func (l *RaftLog) TempGetStabled() uint64 {
	return l.stabled
}
func (l *RaftLog) TempAllEntries() []pb.Entry {
	return l.Entries(l.startIndex(), l.LastIndex()+1)
}
func (l *RaftLog) TempAppliedAndCommitted() (uint64, uint64) {
	return l.applied, l.committed
}
