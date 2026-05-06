package db

import (
	"node/ptypes"
	"strconv"
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
	case ptypes.Op_DECRBY:
		if _, ok := s.store.Data[log.Key]; !ok {
			s.store.Data[log.Key] = &ptypes.Value{
				Type:   ptypes.ValueType_Number,
				IsNull: false,
				Number: 0,
			}
		}

		if s.store.Data[log.Key].Type != ptypes.ValueType_Number {
			if s.store.Data[log.Key].Type != ptypes.ValueType_String {
				delete(s.store.Data, log.Key)
				s.store.Data[log.Key] = &ptypes.Value{
					Type:   ptypes.ValueType_Number,
					IsNull: false,
					Number: 0,
				}
			} else {
				n, err := strconv.ParseInt(s.store.Data[log.Key].String_, 10, 64)
				delete(s.store.Data, log.Key)
				if err != nil {
					s.store.Data[log.Key] = &ptypes.Value{
						Type:   ptypes.ValueType_Number,
						IsNull: false,
						Number: 0,
					}
				} else {
					s.store.Data[log.Key] = &ptypes.Value{
						Type:   ptypes.ValueType_Number,
						IsNull: false,
						Number: n,
					}
				}
			}
		}

		if log.Op == ptypes.Op_INCRBY {
			s.store.Data[log.Key].Number += log.Value.Number
		} else {
			s.store.Data[log.Key].Number -= log.Value.Number
		}
	}

	return true
}
