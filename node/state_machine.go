package main

import (
	"sync"
)

type StateMachine struct {
	Term     int
	LogIndex int
	store    map[string]string
	VotedFor int
	mx       sync.Mutex
}

func NewStateMachine() StateMachine {
	return StateMachine{
		Term:     -1,
		LogIndex: -1,
		store:    map[string]string{},
		VotedFor: -1,
	}
}

func (sm *StateMachine) Set(key, value string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	sm.store[key] = value
}

func (sm *StateMachine) Get(key string) (string, bool) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	value, ok := sm.store[key]

	return value, ok
}

func (sm *StateMachine) Delete(key string) {
	sm.mx.Lock()
	defer sm.mx.Unlock()

	delete(sm.store, key)
}
