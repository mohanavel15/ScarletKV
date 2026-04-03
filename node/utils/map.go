package utils

import (
	"encoding/json"
	"sync"
)

// not happy with sync.Map so here is my own!
type SyncMap[K comparable, V any] struct {
	store map[K]V
	mx    sync.RWMutex
}

func NewSyncMap[K comparable, V any]() SyncMap[K, V] {
	return SyncMap[K, V]{
		store: map[K]V{},
	}
}

func (m *SyncMap[K, V]) Set(key K, value V) {
	m.mx.Lock()
	defer m.mx.Unlock()

	m.store[key] = value
}

func (m *SyncMap[K, V]) Get(key K) (V, bool) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	value, ok := m.store[key]

	return value, ok
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.mx.Lock()
	defer m.mx.Unlock()

	delete(m.store, key)
}

// Danger !!! Should be deep copy....
func (m *SyncMap[K, V]) DumpMap() map[K]V {
	m.mx.RLock()
	defer m.mx.RUnlock()

	return m.store
}

func (m *SyncMap[K, V]) DumpMapJSON() []byte {
	m.mx.RLock()
	defer m.mx.RUnlock()

	data, _ := json.Marshal(m.store)

	return data
}

func (m *SyncMap[K, V]) LoadMap(data []byte) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	return json.Unmarshal(data, &m.store)
}
