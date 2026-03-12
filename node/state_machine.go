package main

import (
	"math/rand/v2"
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
	ip      string
	store   map[string]string
	mx      sync.RWMutex
	timeout int64

	// General States
	term     int64
	logIndex int64
	votedFor string
	state    NodeState

	// Volatile States
	commitIndex int64
	lastApplied int64

	// Volatile Leader States
	nextIndex  map[string]int64
	matchIndex map[string]int64
}

func NewStateMachine(ip string) StateMachine {
	return StateMachine{
		ip:       ip,
		term:     0,
		logIndex: -1,
		store:    map[string]string{},
		votedFor: "",
		state:    FOLLOWER,
		timeout:  rand.Int64N(TIME_RATE) + TIME_RATE,
	}
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

func (sm *StateMachine) GetTerm() int64 {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.term
}

func (sm *StateMachine) SetTerm(term int64) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.term = term
}

func (sm *StateMachine) IncTerm() {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.term += 1
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

func (sm *StateMachine) IncLogIndex() {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.logIndex += 1
}

func (sm *StateMachine) GetVotedFor() string {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.votedFor
}

func (sm *StateMachine) SetVotedFor(votedFor string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.votedFor = votedFor
}

func (sm *StateMachine) Set(key, value string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.store[key] = value
}

func (sm *StateMachine) Get(key string) (string, bool) {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	value, ok := sm.store[key]

	return value, ok
}

func (sm *StateMachine) Delete(key string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	delete(sm.store, key)
}
