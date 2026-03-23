package main

import (
	"fmt"
	"log"
	"net/http"
	pb "node/raft_pb"
	"strings"
	"time"
)

type HTTPHandler struct {
	ip    string
	port  int
	sm    *StateMachine
	distr chan *pb.LogEntry

	server *http.Server
}

func NewHTTPHandler(ip string, port int, sm *StateMachine, distr chan *pb.LogEntry) *HTTPHandler {
	server := http.Server{
		Addr:         fmt.Sprintf("%s:%d", ip, port),
		Handler:      nil,
		ReadTimeout:  time.Second * 3,
		WriteTimeout: time.Second * 3,
		IdleTimeout:  time.Second * 3,
	}

	return &HTTPHandler{
		ip:     ip,
		port:   port,
		sm:     sm,
		distr:  distr,
		server: &server,
	}
}

func (h *HTTPHandler) GetKeys(w http.ResponseWriter, r *http.Request) {
	// keys := make([]string, 0, len(h.sm.store))
	keys := make([]string, 0, 0)
	// for k := range h.sm.Store. {
	// 	keys = append(keys, k)
	// }

	fmt.Fprint(w, strings.Join(keys, "\n"))

}

func (h *HTTPHandler) GetKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]

	value, ok := h.sm.Store.Get(key)

	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	fmt.Fprint(w, value)
}

func (h *HTTPHandler) SetKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]
	fmt.Println("Setting key:", key)

	var buffer [1024]byte
	n, err := r.Body.Read(buffer[:])

	if err != nil && err.Error() != "EOF" {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	value := string(buffer[:n])
	h.sm.Store.Set(key, value)

	h.Distribute(pb.OP_SET, key, value)

	fmt.Fprint(w, "Key set")
}

func (h *HTTPHandler) DeleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]
	fmt.Println("Deleting key:", key)

	h.sm.Store.Delete(key)

	h.Distribute(pb.OP_DELETE, key, "")

	fmt.Fprint(w, "Key deleted")
}

func (h *HTTPHandler) Distribute(op pb.OP, key string, value string) {
	go func() {
		h.distr <- &pb.LogEntry{
			Op:    op,
			Key:   key,
			Value: value,
		}
	}()
}

func (h *HTTPHandler) NonLeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.sm.GetState() == LEADER {
			log.Printf("Request: %s %s", r.Method, r.URL.Path)
			next.ServeHTTP(w, r)
			return
		}

		leaderIP := h.sm.GetLeader()

		if leaderIP == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		url := fmt.Sprintf("http://%s:%d%s", leaderIP, h.port, r.URL.Path)
		http.Redirect(w, r, url, http.StatusPermanentRedirect)
	})
}

func (h *HTTPHandler) ListenAndServe() error {
	handler := http.ServeMux{}

	handler.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.GetKeys(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	handler.HandleFunc("/keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.GetKey(w, r)
		case http.MethodPost:
			h.SetKey(w, r)
		case http.MethodDelete:
			h.DeleteKey(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	h.server.Handler = h.NonLeader(&handler)
	return h.server.ListenAndServe()
}

func (h *HTTPHandler) Close() {
	close(h.distr)
	err := h.server.Close()
	if err != nil {
		log.Printf("Error Closing HTTP Server: %v\n", err)
	}
}
