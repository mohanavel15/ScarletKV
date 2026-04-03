package resp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"node/raft"
	"node/raft_proto"
	"strings"
)

func IsString(value *Value) bool {
	return value.Type == SimpleString || value.Type == BulkString
}

type Handler struct {
	ip       string
	port     int16
	listener net.Listener
	sm       *raft.StateMachine
	distr    chan *raft_proto.LogEntry
}

func NewHandler(ip string, port int16, sm *raft.StateMachine, distr chan *raft_proto.LogEntry) Handler {
	return Handler{
		ip:    ip,
		port:  port,
		sm:    sm,
		distr: distr,
	}
}

func (h *Handler) handleConnection(conn net.Conn) {
	for {
		value, err := Deserilize(conn)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			msg, _ := Serialize(NewError("Invaild RESP Message!"))
			conn.Write([]byte(msg))
		}

		response := h.CommandHandler(value)
		msg, _ := Serialize(response)
		conn.Write([]byte(msg))
	}
}

func (h *Handler) CommandHandler(value *Value) *Value {
	if h.sm.GetState() != raft.LEADER {
		return NewError(fmt.Sprintf("Not Leader, should contact %s", h.sm.GetLeader()))
	}

	if value.Type != Array && len(value.Array) == 0 {
		return NewError("Invaild RESP Command!")
	}

	cmd := value.Array[0]

	if cmd.Type != SimpleString && cmd.Type != BulkString {
		return NewError("Invaild RESP Command!")
	}

	switch cmd.String {
	case "PING":
		return NewBulkString("PONG")
	case "GET":
		if len(value.Array) != 2 || !IsString(value.Array[1]) {
			return NewError("Invaild RESP Command!")
		}

		key := value.Array[1].String

		value_str, ok := h.sm.Store.Get(key)
		if !ok {
			return NewError("Key Not Found")
		}

		value, err := Deserilize(strings.NewReader(value_str))
		if err != nil {
			return NewError("Internal Server Error")
		}

		return value
	case "SET":
		if len(value.Array) != 3 || !IsString(value.Array[1]) {
			return NewError("Invaild RESP Command!")
		}

		key := value.Array[1].String
		value := value.Array[2]

		value_str, _ := Serialize(value)

		h.distr <- &raft_proto.LogEntry{
			Op:    raft_proto.OP_SET,
			Key:   key,
			Value: value_str,
		}

		return NewSimpleString("OK")
	case "DEL":
		if len(value.Array) != 2 || !IsString(value.Array[1]) {
			return NewError("Invaild RESP Command!")
		}

		key := value.Array[1].String

		h.distr <- &raft_proto.LogEntry{
			Op:  raft_proto.OP_DELETE,
			Key: key,
		}

		return NewSimpleString("OK")
	default:
		msg, _ := Serialize(value)
		fmt.Println("================Unknown RESP Cmd================")
		fmt.Println(msg)
		fmt.Println("================================================")
		return NewError("Unknown RESP Command!")
	}
}

func (h *Handler) ListenAndServe() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", h.ip, h.port))
	if err != nil {
		return fmt.Errorf("Error listening:", err)
	}

	h.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting:", err)
			continue
		}

		go h.handleConnection(conn)
	}
}

func (h *Handler) Close() {
	_ = h.listener.Close()
}
