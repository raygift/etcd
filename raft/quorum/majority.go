// Copyright 2019 The etcd Authors
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

package quorum

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// MajorityConfig 是保存了所有节点id 的map 结构；
// 目的是为了判断多数派
// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
type MajorityConfig map[uint64]struct{}

func (c MajorityConfig) String() string {
	sl := make([]uint64, 0, len(c))
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	var buf strings.Builder
	buf.WriteByte('(')
	for i := range sl {
		if i > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprint(&buf, sl[i])
	}
	buf.WriteByte(')')
	return buf.String()
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
func (c MajorityConfig) Describe(l AckedIndexer) string {
	if len(c) == 0 {
		return "<empty majority quorum>"
	}
	type tup struct {
		id  uint64
		idx Index
		ok  bool // idx found?
		bar int  // length of bar displayed for this tup
	}

	// Below, populate .bar so that the i-th largest commit index has bar i (we
	// plot this as sort of a progress bar). The actual code is a bit more
	// complicated and also makes sure that equal index => equal bar.

	n := len(c)
	info := make([]tup, 0, n)
	for id := range c {
		idx, ok := l.AckedIndex(id)
		info = append(info, tup{id: id, idx: idx, ok: ok})
	}

	// Sort by index
	sort.Slice(info, func(i, j int) bool {
		if info[i].idx == info[j].idx {
			return info[i].id < info[j].id
		}
		return info[i].idx < info[j].idx
	})

	// Populate .bar.
	for i := range info {
		if i > 0 && info[i-1].idx < info[i].idx {
			info[i].bar = i
		}
	}

	// Sort by ID.
	sort.Slice(info, func(i, j int) bool {
		return info[i].id < info[j].id
	})

	var buf strings.Builder

	// Print.
	fmt.Fprint(&buf, strings.Repeat(" ", n)+"    idx\n")
	for i := range info {
		bar := info[i].bar
		if !info[i].ok {
			fmt.Fprint(&buf, "?"+strings.Repeat(" ", n))
		} else {
			fmt.Fprint(&buf, strings.Repeat("x", bar)+">"+strings.Repeat(" ", n-bar))
		}
		fmt.Fprintf(&buf, " %5d    (id=%d)\n", info[i].idx, info[i].id)
	}
	return buf.String()
}

// Slice returns the MajorityConfig as a sorted slice.
func (c MajorityConfig) Slice() []uint64 {
	var sl []uint64
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	return sl
}

// 插入排序，从小到大的顺序
func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// 针对当前生效的配置c，通过传入的AckedIndexer 计算已提交index 编号
// CommittedIndex computes the committed index from those supplied via the
// provided AckedIndexer (for the active config).
func (c MajorityConfig) CommittedIndex(l AckedIndexer) Index {
	n := len(c) // 当前config 中记录的集群节点个数
	if n == 0 { // 即使当前config 中记录的节点个数为0，也应该向另外一个config 一样返回已提交编号 committed index；这里选择返回一个特别大的数，从而避免影响 基于两个config 得到的 committed index 中较小值
		// This plays well with joint quorums which, when one half is the zero
		// MajorityConfig, should behave like the other half.
		return math.MaxUint64
	}
	// 当n<=7时，使用位于栈上的局部变量 slice 来汇总commited index。（而 n>7 时手动在堆上分配）
	// 可替代方案时在MajorityConfig 中暂存这个slice，但这种方案影响可用性（众所周知，MajorityConfig 只是一个map）
	//这里基于的假设是，大于7个副本的情况是很少的，因此不必担心大于7 时的性能问题（另外手动分配的性能也不是特别夸张）
	// Use an on-stack slice to collect the committed indexes when n <= 7
	// (otherwise we alloc). The alternative is to stash a slice on
	// MajorityConfig, but this impairs usability (as is, MajorityConfig is just
	// a map, and that's nice). The assumption is that running with a
	// replication factor of >7 is rare, and in cases in which it happens
	// performance is a lesser concern (additionally the performance
	// implications of an allocation here are far from drastic).
	var stk [7]uint64  // 位于栈上的局部数组变量
	var srt []uint64   // 位于栈上的局部slice 变量
	if len(stk) >= n { // n<=7，使用栈上的slice
		srt = stk[:n]
	} else {
		srt = make([]uint64, n) // 手动在堆上分配slice
	}

	{
		// 从右向左逐个为slice 中的位置赋值
		// 赋值内容为某节点id 所确认的 index
		// Fill the slice with the indexes observed. Any unused slots will be
		// left as zero; these correspond to voters that may report in, but
		// haven't yet. We fill from the right (since the zeroes will end up on
		// the left after sorting below anyway).
		i := n - 1
		for id := range c {
			if idx, ok := l.AckedIndex(id); ok {
				srt[i] = uint64(idx)
				i--
			}
		}
	}
	// 使用特殊的插入排序算法对slice 进行排序，使得srt 能够保留在栈上
	// Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
	// package) to keep srt on the stack.
	insertionSort(srt)
	// 数组中最小的index 值被大多数节点认可
	// 换句话说，从slice 的结尾开始，向左侧移动 n/2+1个位置
	// The smallest index into the array for which the value is acked by a
	// quorum. In other words, from the end of the slice, move n/2+1 to the
	// left (accounting for zero-indexing).
	pos := n - (n/2 + 1)
	return Index(srt[pos])
}

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending (i.e. neither a quorum of
// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
// quorum of no has been reached).
func (c MajorityConfig) VoteResult(votes map[uint64]bool) VoteResult {
	if len(c) == 0 {
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.
		return VoteWon
	}

	ny := [2]int{} // vote counts for no and yes, respectively

	var missing int
	for id := range c {
		v, ok := votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			ny[1]++
		} else {
			ny[0]++
		}
	}

	q := len(c)/2 + 1
	if ny[1] >= q {
		return VoteWon
	}
	if ny[1]+missing >= q {
		return VotePending
	}
	return VoteLost
}
