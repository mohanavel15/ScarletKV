package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

type HTTPHandler struct {
	sm *StateMachine
}

func NewHTTPHandler(sm *StateMachine) HTTPHandler {
	return HTTPHandler{
		sm: sm,
	}
}

func (h *HTTPHandler) GetKeys(w http.ResponseWriter, r *http.Request) {
	keys := make([]string, 0, len(h.sm.store))
	for k := range h.sm.store {
		keys = append(keys, k)
	}

	fmt.Fprint(w, strings.Join(keys, "\n"))

}

func (h *HTTPHandler) GetKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]

	value, ok := h.sm.Get(key)

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
	h.sm.Set(key, value)

	fmt.Fprint(w, "Key set")
}

func (h *HTTPHandler) DeleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]
	fmt.Println("Deleting key:", key)

	h.sm.Delete(key)
	fmt.Fprint(w, "Key deleted")
}

func (h *HTTPHandler) ListenAndServe(addr string) error {
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

	server := http.Server{
		Addr:         addr,
		Handler:      &handler,
		ReadTimeout:  time.Second * 3,
		WriteTimeout: time.Second * 3,
		IdleTimeout:  time.Second * 3,
	}

	return server.ListenAndServe()
}
