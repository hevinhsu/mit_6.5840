package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func generateDefaultTimeout() time.Duration {
	return time.Duration(int(500+(rand.Int63()%4500))) * time.Millisecond
}

func (rf *Raft) resetTimer(duration time.Duration) {
	rf.timeoutTimer.Reset(duration)
}

func (rf *Raft) changeToFollower() {
	DPrintf("[serv: %d] [%d] change to follower\n", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.voteFor = None
	// now the timeout is called Election Timeout
	rf.resetTimer(generateDefaultTimeout())
}

func (rf *Raft) changeToCandidate() {
	DPrintf("[serv: %d] [%d] change to candidate\n", rf.me, rf.currentTerm)
	rf.state = Candidate
	rf.startElection()
}

func (rf *Raft) changeToLeader() {
	if rf.state == Leader {
		return
	}
	DPrintf("[serv: %d] [%d] change to leader\n", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.voteFor = None
}

func (rf *Raft) startElection() {

	rf.currentTerm++
	DPrintf("[serv: %d] [%d] start election\n", rf.me, rf.currentTerm)
	var voteCount uint32 = 1

	rf.resetTimer(generateDefaultTimeout())
	rf.voteFor = rf.me

	DPrintf("[serv: %d] [%d] send voteRequest with term: %d\n", rf.me, rf.currentTerm, rf.currentTerm)
	for peer := range rf.peers {

		if peer == rf.me {
			continue
		}

		// send voteRequest to each node
		go func(peer int) {
			rpcRequest := RequestVoteArgs{
				CandidateTerm: rf.currentTerm,
				CandidateId:   rf.me,
			}
			rpcResponse := RequestVoteReply{}

			if !rf.sendRequestVote(peer, &rpcRequest, &rpcResponse) {
				DPrintf("[serv: %d] [%d] send voteRequest to %d failed\n", rf.me, rf.currentTerm, peer)
				return
			}

			if rpcResponse.CurrentTerm == rf.currentTerm {
				if rpcResponse.VoteGranted {
					atomic.AddUint32(&voteCount, 1)
					if int(atomic.LoadUint32(&voteCount)) > len(rf.peers)/2 {
						rf.changeToLeader()
					}
				}
				return
			} else if rpcResponse.CurrentTerm > rf.currentTerm {
				// your term is outdated, become follower
				// reply.Term must not be less than rf.currentTerm
				DPrintf("[serv: %d] [%d] received term: %d from serv: %d. outdated term, become follower\n", rf.me, rf.currentTerm, rpcResponse.CurrentTerm, peer)
				rf.currentTerm = rpcResponse.CurrentTerm
				rf.changeToFollower()
				return
			}
		}(peer)

	}

}
