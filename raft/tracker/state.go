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

// StateType is the state of a tracked follower.
type StateType uint64

const (
	// StateProbe 代表着 follower 的 last index 是未知的。
	// 这类follower 是“被探测的”（也即定期被发送append），用来缩小它 last index （与leader）的差距
	// 在理想的情况下（也是通常情况），因为follower 会反馈一些提示，必须执行一轮探测。
	// 被更多轮次探测的follower 常常是因为已经离线
	// StateProbe indicates a follower whose last index isn't known. Such a
	// follower is "probed" (i.e. an append sent periodically) to narrow down
	// its last index. In the ideal (and common) case, only one round of probing
	// is necessary as the follower will react with a hint. Followers that are
	// probed over extended periods of time are often offline.
	StateProbe StateType = iota
	// StateReplicate 表示follower 渴望接收到 appendEntry 消息发送来的日志记录
	// StateReplicate is the state steady in which a follower eagerly receives
	// log entries to append to its log.
	StateReplicate
	// StateSnapshot 表示follower 需要得到 leader 已经无法提供的日志记录
	// 这类follower 需要从leader 得到完整的snapshot，才可以恢复为 StateReplicate 状态
	// StateSnapshot indicates a follower that needs log entries not available
	// from the leader's Raft log. Such a follower needs a full snapshot to
	// return to StateReplicate.
	StateSnapshot
)

var prstmap = [...]string{
	"StateProbe",
	"StateReplicate",
	"StateSnapshot",
}

func (st StateType) String() string { return prstmap[uint64(st)] }
