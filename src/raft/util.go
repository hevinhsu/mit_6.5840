package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, snapshot StateSnapshot, a ...interface{}) {
	if Debug {
		log.Printf(debugHeader(snapshot)+format, a...)
	}
}

func debugHeader(snapshot StateSnapshot) string {
	var state string
	switch snapshot.State {
	case Follower:
		state = "Follower "
	case Candidate:
		state = "Candidate"
	case Leader:
		state = "Leader   "
	default:
		state = "Unknown"
	}
	return fmt.Sprintf("[serv: %d %v] [%d] ", snapshot.Me, state, snapshot.CurrentTerm)
}
