package raft

import (
	"math/rand/v2"
	"node/ptypes"
	"sync"

	"node/utils"
)

var TIME_RATE int64 = 150

type NodeState int

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

type StateMachine struct {
	ip       string
	mx       sync.RWMutex
	timeout  int64
	leaderIP string

	// General States
	term       int64
	logIndex   int64
	votedFor   string
	state      NodeState
	logEntries []*ptypes.LogEntry

	// Volatile States
	commitIndex int64
	lastApplied int64

	// Volatile Leader States
	NextIndex  utils.SyncMap[string, int64]
	MatchIndex utils.SyncMap[string, int64]
}

func NewStateMachine(ip string) StateMachine {
	return StateMachine{
		ip: ip,
		// Store:    utils.NewSyncMap[string, string](),
		leaderIP: "",
		timeout:  rand.Int64N(TIME_RATE) + TIME_RATE,

		// General States
		term:       0,
		logIndex:   -1,
		votedFor:   "",
		state:      FOLLOWER,
		logEntries: []*ptypes.LogEntry{},

		// Volatile States
		commitIndex: -1,
		lastApplied: -1,

		// Volatile Leader States
		NextIndex:  utils.NewSyncMap[string, int64](),
		MatchIndex: utils.NewSyncMap[string, int64](),
	}
}

func (sm *StateMachine) LogAppend(log *ptypes.LogEntry) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.logEntries = append(sm.logEntries, log)
	sm.logIndex += 1
}

func (sm *StateMachine) LogAppendOrInsertAt(idx int64, log *ptypes.LogEntry) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	if idx < int64(len(sm.logEntries)) {
		sm.logEntries[idx] = log
		sm.logIndex = idx
		return
	}

	sm.logEntries = append(sm.logEntries, log)
	sm.logIndex += 1
}

func (sm *StateMachine) GetId() string {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.ip
}

func (sm *StateMachine) GetState() NodeState {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.state
}

func (sm *StateMachine) SetState(state NodeState) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.state = state
}

func (sm *StateMachine) GetLeader() string {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.leaderIP
}

func (sm *StateMachine) SetLeader(ip string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.leaderIP = ip
}

func (sm *StateMachine) GetTerm() int64 {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.term
}

func (sm *StateMachine) GetPrevLogTerm() int64 {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	if sm.logIndex <= -1 {
		return sm.term
	}

	return sm.logEntries[sm.logIndex].Term
}

func (sm *StateMachine) SetTerm(term int64, votedFor string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	if term <= sm.term {
		return
	}

	sm.term = term
	sm.votedFor = votedFor
}

func (sm *StateMachine) GetLogIndex() int64 {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.logIndex
}

func (sm *StateMachine) GetVotedFor() string {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.votedFor
}

func (sm *StateMachine) GetCommitIndex() int64 {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.commitIndex
}

func (sm *StateMachine) SetCommitIndex(idx int64) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.commitIndex = idx
}
