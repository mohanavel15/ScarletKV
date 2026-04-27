package restapi

import (
	"fmt"
	"log"
	"net/http"
	"node/raft"
	"node/ptypes"
	"os"
	"time"
)

type HTTPHandler struct {
	ip    string
	port  int
	sm    *raft.StateMachine
	distr chan *ptypes.LogEntry

	server *http.Server
}

func NewHTTPHandler(ip string, port int, sm *raft.StateMachine, distr chan *ptypes.LogEntry) *HTTPHandler {
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

func (h *HTTPHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(h.sm.ToJSON())
}

func CORS(next http.Handler) http.Handler {
	// This func from: https://www.stackhawk.com/blog/golang-cors-guide-what-it-is-and-how-to-enable-it/
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *HTTPHandler) ListenAndServe() error {
	handler := http.ServeMux{}

	handler.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.GetStatus(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	handler.HandleFunc("/kill", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, "Okay")
			time.Sleep(100 * time.Millisecond)
			os.Exit(1)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	h.server.Handler = CORS(&handler)
	return h.server.ListenAndServe()
}

func (h *HTTPHandler) Close() {
	close(h.distr)
	err := h.server.Close()
	if err != nil {
		log.Printf("Error Closing HTTP Server: %v\n", err)
	}
}
