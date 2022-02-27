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

import pb "go.etcd.io/etcd/raft/v3/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	offset  uint64 //snapshot 之后的首条日志（在raftlog 中）的index

	logger Logger
}

// 尝试根据unstable 的snapshot 推断出 raftLog 的第一条日志index
// 当存在unstable.snapshot 时，实际正处于 restore snapshot 之后，stableSnapTo 之前
// 因为 unstable 中保存的 snapshot 未来会被执行ApplySnapshot()，从unstable 转移到 storage.snapshot
// 而无论是否执行了ApplySnapshot()，可以确定raftLog 对应的storage 中保存的日志一定是从快照之后开始，因为快照中包含的日志不会再从leader 复制到follower
// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		// unstable.snapshot 是接收到来自leader 发送来的snapshot
		// Metadata.Index 是leader 在调用(ms *MemoryStorage) CreateSnapshot 时传入的index
		// 因此返回 Storage 首个日志的index +1，表示快照之后的首条日志的索引号，所有follower 都需要与raft leader 保持一致
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// 当存在unstable.entries 时，（不存在 snapshot），只需要检查entries 的最后索引号；
// 否则，若存在unstable.snapshot，返回 snapshot.Metadata.Index（snapshot 的data 中日志的最大索引号）
// 若unstable的 entries 和 snapshot 都不存在，则返回0
// ”maybeLastIndex 在至少存在一个unstable.entry 或存在snapshot时，返回last index“
// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil { // 存在unstable snapshot，随后snapshot 将被执行restore，当前
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// 检查在unstable 中索引号为i 的日志的term
// 若 index 大于等于 unstable.offset，则 index 位于unstable.entries 中？
// maybeTerm returns the term of the entry at index i, if there
// is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	// 若 i 小于 unstable.offset，则 i 位于 unstable.entries 之前；
	// 此时可能存在unstable.snapshot，snapshot 中保存的entry 尚未应用到unstable.entries 中

	if i < u.offset { // i 位于当前unstable.entries[0] 之前
		// 若i 恰好等于 snapshot.Metadata.Index，则可以通过Metadata.Term 得到对应的term；
		// 否则即使存在 unstable.snapshot，由于没有snapshot 中其他日志的信息，无法从unstable 中得到对应term
		// 而只有当 unstable.snapshot 存在时，才有机会判断Metadata.Index == i
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false // 无法从unstable （的entries 和 snapshot）获得i 对应的 term
	}
	// 当 i >= unstable.offset，i 位于 unstable entries[0] 之后
	last, ok := u.maybeLastIndex() // 获得unstable 中最大的索引号
	if !ok {                       // 不存在unstable.entries 和snapshot
		return 0, false // 无法获得 i 对应的term
	}
	// 存在 unstable.entries 或 unstable.snapshot
	if i > last { // i 大于 unstable.entries 或unstable.snapshot 的last index
		return 0, false
	}
	// 若 myabeLastIndex 是根据 Unstable.snapshot 得到的
	// 即不存在 unstable.entries，只存在 unstable.snapshot，就不能从 u.entries 获得term吧？
	return u.entries[i-u.offset].Term, true // 从unstable 的entries 中获得 i 对应的term
}

// 在i 对应的日志记录位于 unstable entries 中时，将 untable entries 更新为offset 从i+1 开始的日志部分
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// 如果 i<u.offset，term 与 snapshot 是匹配的（个人理解：因为 unstable.offset = snapshot.Metadata.Index + 1，因此 i<u.offset，表示i <=u.snapshot.Metadata.Index）
	// 只有在term 与至少一个 unstable entry 匹配时才更新 unstable.entries
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:] // 保留 unstable entries 中 index i 之后的部分
		u.offset = i + 1                     // 增大 unstable.offset
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

// 更新 unstable.offset，清空 unstable.entries，保存snapshot
func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1 // snapshot 之后的首条日志（在raftlog 中）的index
	u.entries = nil
	u.snapshot = &s
}

// 判断传入的ents 所包含的日志与unstable 中entries 所持有日志的位置关系
// 若传入的首条记录index 正好是unstable.entries 下一个index，则直接将ents 追加到unstable.entries中
// 若传入的首条记录index 小于等于unsatble 中的首条记录，则将原来已有的未提交记录覆盖
// -- 在进入truncateAndAppend 之前已经判断了 ents[0].index >= commited+1，因此此处当 ents[0].index <=offset 时，[ents[0].index, 当前offset]区间内都是未提交记录，可以被覆盖，覆盖后offset 向前缩小了，因此要更新
// 若传入的首条记录的index 大于 offset，[offset，ents[0].index]之间的记录覆盖
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)): // offset 是相对于 RaftLog 首条记录的偏移量，等式后半部分表示 unstable.entries 的后一条日志的索引号
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...) // 需要append 的日志正好位于 unstable.entries 之后，两部分无缝拼接即可
	case after <= u.offset: // 传入的日志在RaftLog 中的位置 比unstable.entries 都要靠前，则舍弃当前unstable.entries，将传入日志作为新的unstable.entries，并更新 offset值
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after // 缩小 unstable.offset
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		u.logger.Infof("truncate the unstable entries before index %d", after) // 传入的日志与unstable.entries 的日志有重叠，重叠部分以传入的日志为准进行更新
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
