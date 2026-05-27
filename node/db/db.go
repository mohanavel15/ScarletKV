package db

import (
	"encoding/binary"
	"fmt"
	"io"
	"node/ptypes"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const READ_ONLY = os.O_RDONLY
const WRITE_ONLY = os.O_CREATE | os.O_WRONLY
const APPEND_ONLY = os.O_CREATE | os.O_WRONLY | os.O_APPEND

type Store struct {
	path     string
	wal_file *os.File
	store    *ptypes.HashTable
	mx       sync.RWMutex
	timer    *time.Timer
}

func NewStore(path string) *Store {
	s := Store{
		path: path,
		store: &ptypes.HashTable{
			Data: make(map[string]*ptypes.Value),
		},
	}

	s.restore()
	go s.SaveDB()

	return &s
}

func (s *Store) restore() {
	db_path := fmt.Sprintf("%s/db.bin", s.path)
	wal_path := fmt.Sprintf("%s/wal.bin", s.path)

	data, err := os.ReadFile(db_path)
	if err == nil {
		if len(data) > 0 {
			err := proto.Unmarshal(data, s.store)
			if err != nil {
				panic("DB File Corrupted!")
			}
		}
	} else if !os.IsNotExist(err) {
		panic("Unable to read DB file!")
	}

	wal_file, err := os.OpenFile(wal_path, READ_ONLY, 0644)
	if os.IsNotExist(err) {
		s.wal_file, _ = os.OpenFile(wal_path, APPEND_ONLY, 0644)
		return
	} else if err != nil {
		panic("Unable to read WAL file!")
	}
	defer wal_file.Close()

	wal_file.Stat()

	for {
		size_buf := [4]byte{}

		n, err := wal_file.Read(size_buf[:])
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Println("CRIT: WHAT HAPPEND HERE (Read)", err.Error())
		}

		if n != 4 {
			fmt.Println("CRIT: WHAT HAPPEND HERE (Read)", 4)
		}

		size := binary.BigEndian.Uint32(size_buf[:])

		msg_buf := make([]byte, size)

		n, err = wal_file.Read(msg_buf)
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Println("CRIT: WHAT HAPPEND HERE (Read-Msg)", err.Error())
		}

		if n != int(size) {
			fmt.Println("CRIT: WHAT HAPPEND HERE (Read-Msg)", 4, size)
		}

		log := &ptypes.LogEntry{}

		err = proto.Unmarshal(msg_buf, log)
		if err != nil {
			fmt.Println("CRIT: WHAT HAPPEND HERE (Unmarshal)", err.Error())
		}

		s.processLog(log)
	}

	wal_file.Truncate(0)
	s.wal_file, _ = os.OpenFile(wal_path, APPEND_ONLY, 0644)
}

func (s *Store) SaveDB() {
	db_path := fmt.Sprintf("%s/db.bin", s.path)
	tmp_db_path := fmt.Sprintf("%s/tmp_db.bin", s.path)

	duration := time.Second * 5

	if s.timer == nil {
		s.timer = time.NewTimer(duration)
	}

	for {
		s.timer.Reset(duration)
		<-s.timer.C

		s.mx.Lock()

		buffer, _ := proto.Marshal(s.store)
		nfile, _ := os.OpenFile(tmp_db_path, WRITE_ONLY, 0644)
		nfile.Write(buffer)
		nfile.Close()

		err := os.Rename(tmp_db_path, db_path)
		if err != nil {
			panic("Unable able commit.")
		}

		s.wal_file.Truncate(0)
		s.mx.Unlock()
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

	buffer := Marshal(log)
	size, err := s.wal_file.Write(buffer)
	if err != nil {
		fmt.Println("CRIT: WHAT HAPPEND HERE", err.Error(), size, len(buffer))
		return false
	}

	s.processLog(log)

	return true
}

func (s *Store) processLog(log *ptypes.LogEntry) {
	switch log.Op {
	case ptypes.Op_SET:
		s.store.Data[log.Key] = log.Value
	case ptypes.Op_DELETE:
		delete(s.store.Data, log.Key)
	case ptypes.Op_INCRBY, ptypes.Op_DECRBY:
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
}

func (s *Store) Close() {
	_ = s.wal_file.Close()
}

func Marshal(msg proto.Message) []byte {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		panic("Invalid Protobuf Message!")
	}

	buffer := binary.BigEndian.AppendUint32(nil, uint32(len(bytes)))
	buffer = append(buffer, bytes...)

	return buffer
}
