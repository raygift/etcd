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
	"fmt"
	"log"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type raftLog struct {
	// 一条来自应用的日志流向：
	// 1. 从应用传送到leader 的unstable.Entries
	// 2. leader：进入leader 的storage				|		2. follower：从leader 发送到follower 的unstable.Entries

	// storage contains all stable entries since the last snapshot.
	// 保存了自最后一次执行snapshot 之后的所有已经持久化的日志（）
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	logger Logger

	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	maxNextEntsSize uint64
}

// newLog returns log using the given storage and default options. It
// recovers the log to the state that it just commits and applies the
// latest snapshot.
func newLog(storage Storage, logger Logger) *raftLog {
	return newLogWithSize(storage, logger, noLimit)
}

// newLogWithSize returns a log using the given storage and max
// message size.
func newLogWithSize(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage:         storage,
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// MaybeAppend 只用在 handleAppendEntries() 时，用来判断传入的 m.entries 是否可以被append，若可以则将传入的entries 添加到unstable.entries 中
// 若传入的 m.Index 和 m.LogTerm 与RaftLog 中对应 [index,term] 存在冲突，则无需判断 m.Entries，直接返回false，表示无法append
// 否则，判断传入的 m.Entries 与 RaftLog 位置关系
// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	// 首先判断传入的 term 与logTerm 是否与RaftLog 存在冲突；若存在冲突则没有必要继续判断，直接返回false
	if l.matchTerm(index, logTerm) { // index 参数为 ents 的首条日志的index
		// 传入的 term 与 logTerm 与RaftLog 不存在冲突，检查可以append 的日志
		lastnewi = index + uint64(len(ents)) // lastnewi 为传入的ents 的最后一条日志的index
		ci := l.findConflict(ents)
		switch {
		case ci == 0: // 不存在日志冲突，且 RaftLog 中已包含所有传入的ents，无需特殊处理，RaftLog 的 committed 将可能被更新
		case ci <= l.committed: // 存在冲突，且冲突的位置位于已提交日志之前，违背了Raft 论文中已提交日志不能被修改的安全性约束
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed) // 这里一定要 panic 吗？其他网络分区的旧leader 很可能会发送冲突的entries，此时follower 拒绝append 不就可以了？
		default: // ci 大于已提交日志位置，说明传入的ents 中存在 RaftLog 尚未持有的新日志
			offset := index + 1 // 为了ci-offset 与ents 中日志位置对应
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// 向raft 节点的raftlog 中追加日志记录，实际会将待追加的日志添加到unstable.entries，unstable.entries 中的记录会传递给上层应用，由应用控制将日志写入raft 状态机
// 若传入的ents 为空，则直接返回当前lastindex
// 若传入的首条日志索引号小于（已经提交的记录号+1），将被视为非法请求，因此不能覆盖已经提交的记录
// 否则向 unstable.entries 追加记录；追加时若未完成提交的部分已有记录，则旧记录被覆盖重写
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents) // 将append 的 entries 存入 unstable.entries
	return l.lastIndex()
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The index of the given entries MUST be continuously increasing.
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// findConflictByTerm takes an (index, term) pair (indicating a conflicting log
// entry on a leader/follower during an append) and finds the largest index in
// log l with a term <= `term` and an index <= `index`. If no such index exists
// in the log, the log's first index is returned.
//
// The index provided MUST be equal to or less than l.lastIndex(). Invalid
// inputs log a warning and the input index is returned.
func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		l.logger.Warningf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	for {
		logTerm, err := l.term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// hasPendingSnapshot returns if there is pending snapshot waiting for applying.
func (l *raftLog) hasPendingSnapshot() bool {
	return l.unstable.snapshot != nil && !IsEmptySnap(*l.unstable.snapshot)
}

func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

// 由于 Storage 中的日志在上层创建快照时会被Compact 后删除，因此 Storage 中保存的日志是最后一次执行snapshot 之后剩下的日志
// 由于 unstable 的snapshot 来自作为 raft follower/candidate 时收到leader 复制来的snapshot，调用handleSnapshot()
// 若存在unstable.snapshot，则 raftLog 中首条日志索引一定是 snapshot.Metadata.Index+1
// 若 unstable 不存在unstable.snapshot，由于 Storage 中首个日志是dummy entry，而dummy entry 中记录了上一次快照的snapshot.Metadata.Index
// 		因此返回dummy entry.Index +1
func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok { // 判断是否可以从unstable.snapshot 获得 raftLog 的首条日志索引；若存在unstable.snapshot，则 raftLog 中首条日志索引一定是 snapshot.Metadata.Index+1
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

// 若 unstable 存在entries，返回 unstable.entries 最后日志的index；
// 若 unstable 存在snapshot，返回 unstable.snapshot 快照元数据中的index；
// 若 unstable 不存在日志和快照，返回 Storage 最后日志的index。
func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// 获取索引号为 i 的日志的term
// 会首先判断 i 的合法性，若index 小于 dummyIndex 或者大于 lastIndex，则不合法
// 确定索引号合法后，首先在unstable 中
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// 判断 raftLog 中索引 i 位置的日志term 是否与传入的参数term 相同
// 若索引i 位置的日志已被压缩 或 不可用（分别对应 ErrCompacted ErrUnavailable 错误），则返回false
// 若索引 i 位置的日志term 与参数不同，返回fasle
// 否则返回true，表示 raftLog 中索引i 的日志term 与传入参数相同
func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i) // 获取索引 i 位置日志的term
	if err != nil {     // 若i 位置的日志不可用
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// 获取 log 中[lo,hi-1]的日志记录
// 但不保证返回的记录条数一定时 hi-lo 条，因为收到 maxSize 的限制；以及 storage 中可能已经将部分记录compact，此时只能返回尚未截断的部分日志
// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	// 经过 mustCheckOutOfBounds，此时确定 lo >= l.FirstIndex()
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.unstable.offset { // unstable.offset 表示unstable.entries 的首条日志的index，小于 offset 说明 low 对应的日志已经位于 stabled 的storage.entries 中
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted { // 若上次执行完快照，且已经执行完Compacted，则storage 中的此部分日志已经被截断，返回 ErrCompacted
			return nil, err
		} else if err == ErrUnavailable { // storage 中只包含一个 dummy 记录时会返回 unavailable；还有其他情况也会返回此报错；对于此种状态的raftLog 执行slice 会引发painc
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil { // error 是 ErrCompacted 与 ErrUnavailable 之外的其他报错，未定义的情况，程序panic
			panic(err) // TODO(bdarnell)
		}
		// 若上次执行完快照，但尚未执行Compacted，则从 storage 中仍然可以获取到[lo,unstable.offset] 之间的日志记录
		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo { // 获取到的记录条数较少，少于 min(hi, l.unstable.offset)-lo ，则一定不回超过maxSize，直接返回
			return storedEnts, nil
		}

		ents = storedEnts
	}
	if hi > l.unstable.offset { // 若 hi 位于 unstable.offset 之后，则unstable 包含要获取的部分日志，这部分日志是[unstable.offset,hi]
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// 检查 [lo,hi] 的合法性：位于 RaftLog 的 [firstIndex, lastIndex+1] 区间内，且 lo <=hi
// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi { // low > high 显然不合法
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi { // low < firstIndex ，而 firstIndex 之前的都已经被包含在快照后截断了，因此返回 Compacted
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi // 计算从 firstIndex 到 lastIndex 所有日志记录的条数
	if hi > fi+length {              // （貌似没必要计算length，直接判断 high > lastIndex + 1 不就行了？）
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

// err 为nil 则返回 t；err 为 ErrCompacted 则返回0；其他err 则panic
func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
