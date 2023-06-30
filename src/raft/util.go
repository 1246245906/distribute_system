package raft

import (
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Ms2Nano(ms int64) int64 {
	return ms * 1000000
}

// func NewTimestamp() int64 {
// 	return time.Now().UnixNano() + Ms2Nano(TimeoutFixInterval+(rand.Int63()%RandomRetryInterval))
// }
