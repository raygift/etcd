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

package tracker

import (
	"fmt"
	"sort"
	"strings"
)

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
type Progress struct {
	Match, Next uint64
	// State defines how the leader should interact with the follower.
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	State StateType

	// PendingSnapshot is used in StateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	//
	// TODO(tbg): the leader should always have this set to true.
	RecentActive bool

	// ProbeSent is used while this follower is in StateProbe. When ProbeSent is
	// true, raft should pause sending replication message to this peer until
	// ProbeSent is reset. See ProbeAcked() and IsPaused().
	ProbeSent bool

	// Inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is Full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
	Inflights *Inflights

	// IsLearner is true if this progress is tracked for a learner.
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting ProbeSent,
// PendingSnapshot, and Inflights.
func (pr *Progress) ResetState(state StateType) {
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// 重置 pr.ProbeSent 状态为false，指示leader 后续append 消息可以无延迟地被继续发送
// 在 follower 接收了一次appendentries RPC 时，ProbeAcked() 被调用
// ProbeAcked is called when this peer has accepted an append. It resets
// ProbeSent to signal that additional append messages should be sent without
// further delay.
func (pr *Progress) ProbeAcked() {
	pr.ProbeSent = false
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == StateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// leader 收到follower 响应AppendEntries 的消息，且响应消息不是reject，
// 则follower 已经确认收到leader 同步的entries；
// 此时 leader 调用 MaybeUpdate；
// 当传入的参数n 大于follower match 的index时，返回true并更新 match，
// 否则返回false，同时更新后续要传递给follower 的日志编号next
// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n { // 若n 大于 已知match 的index，则更新 match index
		pr.Match = n
		updated = true
		pr.ProbeAcked() // 重置标志，告知leader 可以继续发送后续append 消息
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// 乐观更新，将pr.Next 更新为 n+1
// OptimisticUpdate signals that appends all the way up to and including index n
// are in-flight. As a result, Next is increased to n+1.
func (pr *Progress) OptimisticUpdate(n uint64) { pr.Next = n + 1 }

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index of the append message rejected by the follower, and
// the hint that we want to decrease to.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
//
// 根据follower 所拒绝的 index 和拒绝时提供的 hintIndex 更新节点对应记录的 next
// 由于当append 的日志顺序错误或重复时，也会导致follower 响应reject，而此时并不需要更新next，因此直接返回false
// 如果reject 代表了 follower 确实发现了日志冲突（即不是上述顺序错误或重复发送导致），则减小Next，
// Progress 被清空？
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == StateReplicate { // 已经处于StateReplicate 状态，说明此节点没有拒绝leader 的appendEntries 请求
		// 对于此状态的节点，只需根据match 更新 Next 即可

		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match { // 被reject 的index 已经完成了match，因此无需根据reject 更新 next
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use matchHint if it's larger?
		pr.Next = pr.Match + 1
		return true
	}

	// 处于 StateProbe 或 StateSnapshot 状态的节点
	// 还在尝试找到正确的 Next
	//
	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	if pr.Next-1 != rejected { // 正常情况下 rejected 即leader 发送AppendEntries 时的m.Index，当时赋值为 next-1；因此若与 next-1 不同，则next 已经更新过，无需再次更新
		return false
	}
	// 取被拒绝的index 原本在 mayAppend() 中被设置为 Next-1，若被拒绝的index < matchHint+1，则 Next = Next -1 ，即 Next 向前退 1 个位置。
	// 这是StateProbe 状态时每次执行MaybeDecrTo 得到的最小位移
	// 若 matchHint+1 < 被拒绝的index，则Next 向前的位置肯定大于1
	// matchHint 的值共经历过两次优化尝试
	// 第一次优化：在follower 得到append 来的index 后，当决定拒绝时，follower 会尝试找到index 和term 均比append 来的首条日志小或相等的日志，即针对如下情况
	//   idx        1 2 3 4 5 6 7 8 9
	//              -----------------
	//   term (L)   1 3 3 3 3 3 3 3 7
	//   term (F)   1 3 3 4 4 5 5 5 6
	// 因为针对同一个index，若follower 的日志term 更大，而由于日志的 term 是单调不减的，因此 follower 中 index 之前所有大于append 来的term 的记录，均与leader 中相同index 的记录冲突
	// 上图中，当leader 发送index == 9 的记录给follower 时被拒绝，将next 减1 再次尝试发送index==8 的记录同样被拒绝，
	// 此时因为 leader  index == 8 之前的记录的term一定小于等于3
	// 因此   follower index == 8 之前，所有term > 3 的记录，均与leader 的冲突
	// follower 中最后一个term <= 3 的日志是 index == 3 的记录，因此follower 将 3 作为 hintIndex

	// 第二次优化：当leader 得到follower 响应的 hintIndex 后，若hintIndex 的hintTerm < append 到follower 的首条日志的 term，由于可以确定 follower 更小的日志均 <= hintTerm，
	// 因此leader 中大于hintTerm 的index 处日志均需要同步给follower，如下图所示
	// leader 尝试向follower 发送 index == 7 的日志，被follower 拒绝，且 hintIndex == 5， hintTerm == 2
	// leader 便可知道自己日志中 term >2 的日志均需要同步给follower，因此将 index 减小到 term <= 2 的日志位置
	//   idx        1 2 3 4 5 6 7 8 9
	//              -----------------
	//   term (L)   1 3 3 3 5 5 5 5 5
	//   term (F)   1 1 1 1 2 2
	//

	pr.Next = max(min(rejected, matchHint+1), 1)
	pr.ProbeSent = false
	return true
}

// 用来判断progress 对应的节点是否已经被限流
// 当一个节点拒绝了最近一次的MsgApps 消息时，代表它当前正处于等待快照的阶段，或者节点已经达到 MaxInflightMsgs 的上限时，会被限流
// 除这两种情况之外的正常情况下，IsPaused 返回false
// leader 会降低与被限流节点的通信频率，直到该节点再次恢复到可以接收稳定日志流的状态
// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.ProbeSent
	case StateReplicate:
		return pr.Inflights.Full()
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprintf(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[uint64]*Progress

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}
