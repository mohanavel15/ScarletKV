package main

import (
	"math/rand"
	"sync"
)

type NodeState int

const (
	UNKNOWN NodeState = iota
	FOLLOWER
	CANDIDATE
	LEADER
)

type StateMachine struct {
	id       int64
	term     int64
	logIndex int64
	store    map[string]string
	votedFor int64
	state    NodeState
	timeout  int64
	mx       sync.RWMutex
}

func NewStateMachine() StateMachine {
	return StateMachine{
		term:     -1,
		logIndex: -1,
		store:    map[string]string{},
		votedFor: -1,
		state:    UNKNOWN,
		timeout:  rand.Int63n(150) + 150, // 150-300ms
	}
}

func (sm *StateMachine) GetId() int64 {
	return sm.id
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

func (sm *StateMachine) GetVotedFor() int64 {
	sm.mx.RLock()
	defer sm.mx.RUnlock()

	return sm.votedFor
}

func (sm *StateMachine) SetVotedFor(votedFor int64) {
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
