package raft

import (
	"fmt"
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

type StateSnapshot struct {
	CurrentTerm int
	VoteFor     int
	State       NodeState
	Me          int
}

func cloneState(rf *Raft) StateSnapshot {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return StateSnapshot{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		State:       rf.state,
		Me:          rf.me,
	}

}

func (rf *Raft) resetElectionTimeout(duration time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout.Reset(duration)
}

func (rf *Raft) changeToFollower(newTerm int, voteFor int) {
	// already change state to follower
	raftStateSnapshot := cloneState(rf)
	if raftStateSnapshot.State == Follower && raftStateSnapshot.CurrentTerm == newTerm && raftStateSnapshot.VoteFor == None {
		fmt.Println("QQ ignore follower change because already follower")
		return
	}

	if raftStateSnapshot.State != Follower {
		DPrintf("change to follower\n", raftStateSnapshot)
	}
	//DPrintf("[serv: %d] [%d] change to follower\n", rf.me, raftStateSnapshot.CurrentTerm)
	// write shared memory
	rf.mu.Lock()
	rf.state = Follower
	rf.voteFor = voteFor
	rf.currentTerm = newTerm
	// now the timeout is called Election Timeout
	rf.heartBeatsTimeout.Stop()
	rf.mu.Unlock()

	rf.resetElectionTimeout(generateElectionTimeout())
}

func (rf *Raft) changeToCandidate() {
	snapshot := cloneState(rf)
	rf.mu.Lock()
	//if rf.state == Candidate {
	//	DPrintf("no Leader creation. Restart election\n", snapshot)
	//} else {
	//	DPrintf("change to candidate, start election\n", snapshot)
	//}

	DPrintf("change to candidate, start election\n", snapshot)
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.mu.Unlock()
	rf.resetElectionTimeout(generateElectionTimeout())

	snapshot = cloneState(rf)
	var voteCount uint32 = 1

	DPrintf("send voteRequest with term: %d\n", snapshot, snapshot.CurrentTerm)
	for peer := range rf.peers {

		if peer == rf.me {
			continue
		}

		// send voteRequest to each node
		go func(peer int) {
			snapshot := snapshot
			rpcRequest := RequestVoteArgs{
				CandidateTerm: snapshot.CurrentTerm,
				CandidateId:   rf.me,
			}
			rpcResponse := RequestVoteReply{}

			if !rf.sendRequestVote(peer, &rpcRequest, &rpcResponse) {
				DPrintf("send voteRequest to %d failed. req.term: %d, resp.term: %d\n", snapshot, peer, rpcRequest.CandidateTerm, rpcResponse.CurrentTerm)
				return
			}
			DPrintf("received voteRequest from %d with: %v\n", snapshot, peer, rpcResponse.VoteGranted)
			snapshot = cloneState(rf)
			if rpcResponse.CurrentTerm == snapshot.CurrentTerm {
				// cut when server already become leader
				if snapshot.State == Leader {
					return
				}

				if rpcResponse.VoteGranted {
					atomic.AddUint32(&voteCount, 1)
					if int(atomic.LoadUint32(&voteCount)) > len(rf.peers)/2 {
						rf.changeToLeader()
					} else {
						DPrintf("received vote from %d, but not enough. current vote: %d\n", snapshot, peer, int(atomic.LoadUint32(&voteCount)))
					}
				}
				return
			} else if rpcResponse.CurrentTerm > snapshot.CurrentTerm {
				// your term is outdated, become follower
				// reply.Term must not be less than rf.currentTerm
				DPrintf("received term: %d from serv: %d. outdated term, become follower\n", snapshot, rpcResponse.CurrentTerm, peer)
				rf.changeToFollower(rpcResponse.CurrentTerm, None)
				return
			} else {
				DPrintf("received term: %d from serv: %d. I don't know what happened, ignore\n", snapshot, rpcResponse.CurrentTerm, peer)
			}
		}(peer)

	}
}

func (rf *Raft) changeToLeader() {

	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Leader
	rf.voteFor = None
	rf.electionTimeout.Stop()
	rf.mu.Unlock()
	stateSnapshot := cloneState(rf)
	DPrintf("change to leader\n", stateSnapshot)

	rf.broadcastHeartbeat()
}

func (rf *Raft) ResetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartBeatsTimeout.Reset(generateHeartBeatsTimeout())
}

type RequestHeartbeatArgs struct {
	Term int
}

type RequestHeartbeatReply struct {
}

func (rf *Raft) RequestHeartbeat(args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) {
	stateSnapshot := cloneState(rf)
	if args.Term < stateSnapshot.CurrentTerm {
		rf.resetElectionTimeout(generateElectionTimeout())
		DPrintf("received heartbeat from %d with outdated term: %d. ignore\n", stateSnapshot, stateSnapshot.Me, args.Term)
		return
	}

	if args.Term > stateSnapshot.CurrentTerm {
		DPrintf("received heartbeat from %d with newer term: %d. become follower\n", stateSnapshot, stateSnapshot.Me, args.Term)
		rf.changeToFollower(args.Term, None)
		return
	}
	DPrintf("received heartbeat from %d. Do Nothing\n", stateSnapshot, stateSnapshot.Me)
}

func (rf *Raft) sendRequestHeartbeat(server int, args *RequestHeartbeatArgs, reply *RequestHeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	stateSnapshot := cloneState(rf)
	if stateSnapshot.State != Leader {
		return
	}

	//DPrintf("broadcast heartbeat\n", stateSnapshot)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rpcRequest := RequestHeartbeatArgs{
				Term: stateSnapshot.CurrentTerm,
			}
			rpcResponse := RequestHeartbeatReply{}
			if !rf.sendRequestHeartbeat(peer, &rpcRequest, &rpcResponse) {
				DPrintf("send heartbeat to %d failed\n", stateSnapshot, peer)
				return
			}
		}(peer)
	}
	rf.ResetHeartbeatTimer()
}
