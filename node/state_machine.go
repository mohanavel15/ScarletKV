package main

import (
	"math/rand/v2"
	"sync"
)

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
		timeout:  rand.Int64N(150) + 150, // 150-300ms
	}
}

func (sm *StateMachine) GetId() string {
	return sm.ip
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
