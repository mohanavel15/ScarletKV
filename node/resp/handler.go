package resp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"node/ptypes"
	"node/raft"
	"strconv"
)

func IsString(value *Value) bool {
	return value.Type == SimpleString || value.Type == BulkString
}

type Handler struct {
	ip       string
	port     int16
	listener net.Listener
	sm       *raft.StateMachine
	getFunc  func(string) (*ptypes.Value, bool)
	distr    chan *raft.Message

	cmdRegister map[string]func(*Value) *Value
}

func NewHandler(ip string, port int16, sm *raft.StateMachine, distr chan *raft.Message, getFunc func(string) (*ptypes.Value, bool)) Handler {
	return Handler{
		ip:      ip,
		port:    port,
		sm:      sm,
		distr:   distr,
		getFunc: getFunc,
	}
}

func (h *Handler) handleConnection(conn net.Conn) {
	defer conn.Close()

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

		value, ok := h.getFunc(key)
		if !ok {
			return NewError("Key Not Found")
		}

		return ProtoVal2RESP(value)
	case "SET":
		if len(value.Array) != 3 || !IsString(value.Array[1]) {
			return NewError("Invaild RESP Command!")
		}

		key := value.Array[1].String
		val := value.Array[2]

		msg := raft.NewMessage(ptypes.Op_SET, key, RESP2ProtoVal(val))
		h.distr <- msg

		if msg.WaitForConfirmation() {
			return NewSimpleString("OK")
		} else {
			return NewError("Something went wrong, try again...")
		}

	case "DEL":
		if len(value.Array) != 2 || !IsString(value.Array[1]) {
			return NewError("Invaild RESP Command!")
		}

		key := value.Array[1].String

		msg := raft.NewMessage(ptypes.Op_DELETE, key, nil)

		h.distr <- msg

		if msg.WaitForConfirmation() {
			return NewSimpleString("OK")
		} else {
			return NewError("Something went wrong, try again...")
		}
	case "INCRBY":
		key := value.Array[1].String
		val := value.Array[2]

		if val.Type != Integer && val.Type != BulkString && val.Type != SimpleString {
			return NewError("Increament By offset should be a number!")
		} else if val.Type == BulkString || val.Type == SimpleString {
			nint, err := strconv.ParseInt(val.String, 10, 64)
			if err != nil {
				return NewError("Increament By offset should be a number!")
			}

			val.String = ""
			val.Integer = nint
			val.Type = Integer
		}

		msg := raft.NewMessage(ptypes.Op_INCRBY, key, RESP2ProtoVal(val))
		h.distr <- msg

		if msg.WaitForConfirmation() {
			return NewSimpleString("OK")
		} else {
			return NewError("Something went wrong, try again...")
		}
	case "DECRBY":
		key := value.Array[1].String
		val := value.Array[2]

		if val.Type != Integer && val.Type != BulkString && val.Type != SimpleString {
			return NewError("Decreament By offset should be a number!")
		} else if val.Type == BulkString || val.Type == SimpleString {
			nint, err := strconv.ParseInt(val.String, 10, 64)
			if err != nil {
				return NewError("Decreament By offset should be a number!")
			}

			val.String = ""
			val.Integer = nint
			val.Type = Integer
		}

		msg := raft.NewMessage(ptypes.Op_DECRBY, key, RESP2ProtoVal(val))
		h.distr <- msg

		if msg.WaitForConfirmation() {
			return NewSimpleString("OK")
		} else {
			return NewError("Something went wrong, try again...")
		}
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
