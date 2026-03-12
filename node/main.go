package main

import (
	"log"
	"net"
	"os"
)

var nodes = map[string]int64{
	"10.150.3.2": -1,
	"10.150.3.3": -1,
	"10.150.3.4": -1,
	"10.150.3.5": -1,
	"10.150.3.6": -1,
}

func main() {
	ip := PrintStartUpInfo()
	delete(nodes, ip)

	sm := NewStateMachine(ip)

	handler := NewHTTPHandler(&sm)
	go handler.ListenAndServe(":8080")

	raft_handler := NewRAFTServer(&sm)
	raft_handler.ListenAndServe(":6000")
}

func PrintStartUpInfo() string {
	host, _ := os.Hostname()
	ips, _ := net.LookupIP(host)
	log.Printf("Hello from %s at %s\n", host, ips[0])

	return ips[0].String()
}
