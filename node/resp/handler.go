package resp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"node/utils"
	"sync"
)

func IsString(value *Value) bool {
	return value.Type == SimpleString || value.Type == BulkString
}

type Handler struct {
	ip          string
	port        int16
	listener    net.Listener
	Middleware  *utils.Middleware[*Value]
	cmdRegister map[string]func(*Value) *Value
	mx          *sync.RWMutex
}

func NewHandler(ip string, port int16) *Handler {
	s := Handler{
		ip:          ip,
		port:        port,
		Middleware:  utils.NewMiddleware[*Value](),
		cmdRegister: make(map[string]func(*Value) *Value),
	}

	return &s
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

		response := h.Middleware.Execute(value, h.CommandHandler)

		msg, _ := Serialize(response)
		conn.Write([]byte(msg))
	}
}

func (h *Handler) RegisterCommand(cmd string, handlerFunc func(*Value) *Value) {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.cmdRegister[cmd] = handlerFunc
}

func (h *Handler) CommandHandler(value *Value) *Value {
	if value.Type != Array && len(value.Array) == 0 {
		return NewError("Invaild RESP Command!")
	}

	cmd := value.Array[0]

	if cmd.Type != SimpleString && cmd.Type != BulkString {
		return NewError("Invaild RESP Command!")
	}

	h.mx.RLock()
	defer h.mx.RUnlock()

	if handlerFunc, ok := h.cmdRegister[cmd.String]; ok {
		return handlerFunc(value)
	} else {
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
