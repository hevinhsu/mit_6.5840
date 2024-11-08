package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func generateElectionTimeout() time.Duration {
	return time.Duration(500+rand.Int()%4500) * time.Millisecond
}

func generateHeartBeatsTimeout() time.Duration {
	return time.Duration(50+rand.Int()%500) * time.Millisecond
}

func (rf *Raft) resetElectionTimeout(duration time.Duration) {
	rf.electionTimeout.Reset(duration)
}

func (rf *Raft) changeToFollower() {
	if rf.state == Follower {
		return
	}

	DPrintf("[serv: %d] [%d] change to follower\n", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.voteFor = None
	// now the timeout is called Election Timeout
	rf.heartBeatsTimeout.Stop()
	rf.resetElectionTimeout(generateElectionTimeout())
}

func (rf *Raft) changeToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[serv: %d] [%d] change to candidate\n", rf.me, rf.currentTerm)
	rf.state = Candidate
	rf.startElection()
}

func (rf *Raft) changeToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	DPrintf("[serv: %d] [%d] change to leader\n", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.voteFor = None
	rf.electionTimeout.Stop()
	rf.ResetHeartbeatTimer()

	rf.broadcastHeartbeat()
}

func (rf *Raft) ResetHeartbeatTimer() {
	rf.heartBeatsTimeout.Reset(generateHeartBeatsTimeout())
}

type RequestHeartbeatArgs struct {
	Term int
}

type RequestHeartbeatReply struct {
}

func (rf *Raft) RequestHeartbeat(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		rf.resetElectionTimeout(generateElectionTimeout())
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeToFollower()
		return
	}
}

func (rf *Raft) sendRequestHeartbeat(server int, args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	if rf.state != Leader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader {
			return
		}
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			rpcRequest := RequestHeartbeatArgs{
				Term: rf.currentTerm,
			}
			rf.mu.Unlock()
			rpcResponse := RequestHeartbeatReply{}
			if !rf.sendRequestHeartbeat(peer, &rpcRequest, &rpcResponse) {
				rf.mu.Lock()
				DPrintf("[serv: %d] [%d] send heartbeat to %d failed\n", rf.me, rf.currentTerm, peer)
				rf.mu.Unlock()
				return
			}
		}(peer)
	}
	rf.ResetHeartbeatTimer()
}

func (rf *Raft) startElection() {

	rf.currentTerm++
	DPrintf("[serv: %d] [%d] start election\n", rf.me, rf.currentTerm)
	var voteCount uint32 = 1

	rf.resetElectionTimeout(generateElectionTimeout())
	rf.voteFor = rf.me

	DPrintf("[serv: %d] [%d] send voteRequest with term: %d\n", rf.me, rf.currentTerm, rf.currentTerm)
	for peer := range rf.peers {

		if peer == rf.me {
			continue
		}

		// send voteRequest to each node
		go func(peer int) {
			rf.mu.Lock()
			rpcRequest := RequestVoteArgs{
				CandidateTerm: rf.currentTerm,
				CandidateId:   rf.me,
			}
			rf.mu.Unlock()
			rpcResponse := RequestVoteReply{}

			if !rf.sendRequestVote(peer, &rpcRequest, &rpcResponse) {
				rf.mu.Lock()
				DPrintf("[serv: %d] [%d] send voteRequest to %d failed\n", rf.me, rf.currentTerm, peer)
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			if rpcResponse.CurrentTerm == currentTerm {
				if rpcResponse.VoteGranted {
					atomic.AddUint32(&voteCount, 1)
					if int(atomic.LoadUint32(&voteCount)) > len(rf.peers)/2 {
						rf.changeToLeader()
					}
				}
				return
			} else if rpcResponse.CurrentTerm > currentTerm {
				// your term is outdated, become follower
				// reply.Term must not be less than rf.currentTerm
				DPrintf("[serv: %d] [%d] received term: %d from serv: %d. outdated term, become follower\n", rf.me, currentTerm, rpcResponse.CurrentTerm, peer)
				rf.currentTerm = rpcResponse.CurrentTerm
				rf.changeToFollower()
				return
			}
		}(peer)

	}

}
