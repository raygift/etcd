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
	"math"
	"strconv"
)

// Index is a Raft log position.
type Index uint64

func (i Index) String() string {
	if i == math.MaxUint64 {
		return "∞"
	}
	return strconv.FormatUint(uint64(i), 10)
}

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
type AckedIndexer interface {
	AckedIndex(voterID uint64) (idx Index, found bool)
}

type mapAckIndexer map[uint64]Index // mapAckIndexer 只是一个 map[] 类型，因此下方 AckedIndex 可以对任意 map[uint64]Index 执行；在判断节点已经接收到的最大日志index 时，AckedIndex 被应用于确认follower 已acked 的最大index，位于/etcd/raft/tracker/tracker.go：func (p *ProgressTracker) Committed() uint64

// follower 在对leader 发送的appendEntry 进行响应确认，确认中包含的已同步的index 被记录到一个map里
// 若节点 id 已对接收到的 index 进行过ack 确认，则在 mapAckIndexer 中会存在 m[id]=index 这样一条映射
func (m mapAckIndexer) AckedIndex(id uint64) (Index, bool) {
	idx, ok := m[id]
	return idx, ok
}

// VoteResult indicates the outcome of a vote.
//
//go:generate stringer -type=VoteResult
type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)
