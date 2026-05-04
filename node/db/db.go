package db

import (
	"node/ptypes"
	"sync"
)

type Store struct {
	store ptypes.HashTable
	mx    sync.RWMutex
}

func NewStore() Store {
	return Store{
		store: ptypes.HashTable{
			Data: make(map[string]*ptypes.Value),
		},
	}
}

func (s *Store) Get(key string) (*ptypes.Value, bool) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	value, ok := s.store.Data[key]

	return value, ok
}

func (s *Store) Commit(log *ptypes.LogEntry) bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	switch log.Op {
	case ptypes.Op_SET:
		s.store.Data[log.Key] = log.Value
	case ptypes.Op_DELETE:
		delete(s.store.Data, log.Key)
	case ptypes.Op_INCRBY:
		if s.store.Data[log.Key].Type != ptypes.ValueType_Number {
			panic("Should be unreachable!")
		}

		s.store.Data[log.Key].Number += log.Value.Number
	case ptypes.Op_DECRBY:
		if s.store.Data[log.Key].Type != ptypes.ValueType_Number {
			panic("Should be unreachable!")
		}

		s.store.Data[log.Key].Number -= log.Value.Number
	}

	return true
}
