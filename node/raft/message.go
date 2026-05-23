package raft

import (
	"node/ptypes"
	"time"
)

type Message struct {
	log     *ptypes.LogEntry
	success chan bool
	// hasTimedOut bool

	// mx sync.Mutex
}

func NewMessage(op ptypes.Op, key string, val *ptypes.Value) *Message {
	return &Message{
		log: &ptypes.LogEntry{
			Op:       op,
			Key:      key,
			Value:    val,
			Deadline: time.Now().Add(10 * time.Second).UnixMilli(), // Wait for 10 seconds
		},
		success: make(chan bool, 1),
		// hasTimedOut: false,
	}
}

func (m *Message) WaitForConfirmation() bool {
	timer := time.NewTimer(time.Duration(m.log.Deadline-time.Now().UnixMilli()) * time.Millisecond)
	defer timer.Stop()

	select {
	case val := <-m.success:
		return val
	case <-timer.C:
		return false
	}
}
