package main

import (
	"encoding/json"
	pb "node/raft_pb"
	"sync"
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
	Store    SyncMap[string, string]
	mx       sync.RWMutex
	timeout  int64
	leaderIP string

	// General States
	term       int64
	logIndex   int64
	votedFor   string
	state      NodeState
	logEntries []*pb.LogEntry

	// Volatile States
	commitIndex int64
	lastApplied int64

	// Volatile Leader States
	NextIndex  SyncMap[string, int64]
	MatchIndex SyncMap[string, int64]
}

func NewStateMachine(ip string) StateMachine {
	return StateMachine{
		ip:       ip,
		Store:    NewSyncMap[string, string](),
		leaderIP: "",

		// General States
		term:       0,
		logIndex:   -1,
		votedFor:   "",
		state:      FOLLOWER,
		logEntries: []*pb.LogEntry{},

		// Volatile States
		commitIndex: -1,
		lastApplied: -1,

		// Volatile Leader States
		NextIndex:  NewSyncMap[string, int64](),
		MatchIndex: NewSyncMap[string, int64](),
	}
}

func (sm *StateMachine) LogAppend(log *pb.LogEntry) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.logEntries = append(sm.logEntries, log)
	sm.logIndex += 1
}

func (sm *StateMachine) LogAppendOrInsertAt(idx int64, log *pb.LogEntry) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	if idx < int64(len(sm.logEntries)) {
		sm.logEntries[idx] = log
	}

	sm.logEntries = append(sm.logEntries, log)
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

func (sm *StateMachine) SetLogIndex(logIndex int64) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.logIndex = logIndex
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

func (sm *StateMachine) ToJSON() []byte {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	sme := StateMachineExport{
		Term:        sm.term,
		LogIndex:    sm.logIndex,
		VotedFor:    sm.votedFor,
		State:       sm.state,
		LogEntries:  sm.logEntries,
		Store:       sm.Store.store,
		CommitIndex: sm.commitIndex,
		Timeout:     sm.timeout,
	}

	data, _ := json.Marshal(sme)

	return data
}

type StateMachineExport struct {
	Term        int64             `json:"term"`
	LogIndex    int64             `json:"log_index"`
	State       NodeState         `json:"state"`
	VotedFor    string            `json:"voted_for"`
	CommitIndex int64             `json:"commit_index"`
	LogEntries  []*pb.LogEntry    `json:"logs"`
	Store       map[string]string `json:"store"`
	Timeout     int64             `json:"timeout"`
}
