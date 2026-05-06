package main

import (
	"fmt"
	"node/db"
	"node/ptypes"
	"node/raft"
	"node/resp"
	"strconv"
)

type Scarlet struct {
	store *db.Store
	raft  *raft.Raft
	resp  *resp.Handler
}

func NewScarlet(store *db.Store, raft *raft.Raft, resp *resp.Handler) *Scarlet {
	s := Scarlet{
		store: store,
		raft:  raft,
		resp:  resp,
	}

	s.resp.Middleware.Add(s.LeaderMiddleware)

	s.resp.RegisterCommand("PING", s.Ping)
	s.resp.RegisterCommand("GET", s.Get)
	s.resp.RegisterCommand("SET", s.Set)
	s.resp.RegisterCommand("DEL", s.Delete)
	s.resp.RegisterCommand("INCRBY", s.ChanageBy)
	s.resp.RegisterCommand("DECRBY", s.ChanageBy)

	return &s
}

func (s *Scarlet) LeaderMiddleware(value *resp.Value, next func(*resp.Value) *resp.Value) *resp.Value {
	if s.raft.SM().GetState() != raft.LEADER {
		return resp.NewError(fmt.Sprintf("Not Leader, should contact %s", s.raft.SM().GetLeader()))
	}

	return next(value)
}

func (s *Scarlet) Ping(value *resp.Value) *resp.Value {
	return resp.NewSimpleString("PONG")
}

func (s *Scarlet) Get(value *resp.Value) *resp.Value {
	if len(value.Array) != 2 || !resp.IsString(value.Array[1]) {
		return resp.NewError("Invaild RESP Command!")
	}

	key := value.Array[1].String

	val, ok := s.store.Get(key)
	if !ok {
		return resp.NewError("Key Not Found")
	}

	return ProtoVal2RESP(val)
}

func (s *Scarlet) Set(value *resp.Value) *resp.Value {
	if len(value.Array) != 3 || !resp.IsString(value.Array[1]) {
		return resp.NewError("Invaild RESP Command!")
	}

	key := value.Array[1].String
	val := value.Array[2]

	msg := raft.NewMessage(ptypes.Op_SET, key, RESP2ProtoVal(val))
	s.raft.DistributorC <- msg

	if msg.WaitForConfirmation() {
		return resp.NewSimpleString("OK")
	} else {
		return resp.NewError("Something went wrong, try again...")
	}
}

func (s *Scarlet) Delete(value *resp.Value) *resp.Value {
	if len(value.Array) != 2 || !resp.IsString(value.Array[1]) {
		return resp.NewError("Invaild RESP Command!")
	}

	key := value.Array[1].String

	msg := raft.NewMessage(ptypes.Op_DELETE, key, nil)
	s.raft.DistributorC <- msg

	if msg.WaitForConfirmation() {
		return resp.NewSimpleString("OK")
	} else {
		return resp.NewError("Something went wrong, try again...")
	}
}

func (s *Scarlet) ChanageBy(value *resp.Value) *resp.Value {
	cmd := value.Array[0].String
	key := value.Array[1].String
	val := value.Array[2]

	if val.Type != resp.Integer && !resp.IsString(val) {
		return resp.NewError("Offset amount should be a number!")
	} else if resp.IsString(val) {
		nint, err := strconv.ParseInt(val.String, 10, 64)
		if err != nil {
			return resp.NewError("Increament By offset should be a number!")
		}

		val.String = ""
		val.Integer = nint
		val.Type = resp.Integer
	}

	var op ptypes.Op

	if cmd == "INCRBY" {
		op = ptypes.Op_INCRBY
	}

	if cmd == "DECRBY" {
		op = ptypes.Op_DECRBY
	}

	msg := raft.NewMessage(op, key, RESP2ProtoVal(val))
	s.raft.DistributorC <- msg

	if msg.WaitForConfirmation() {
		return resp.NewSimpleString("OK")
	} else {
		return resp.NewError("Something went wrong, try again...")
	}
}
