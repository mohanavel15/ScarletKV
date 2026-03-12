package main

import (
	"log"
	"net"
	"os"
)

var nodes = map[string]bool{
	"10.150.3.2": true,
	"10.150.3.3": true,
	"10.150.3.4": true,
	"10.150.3.5": true,
	"10.150.3.6": true,
}

func main() {
	ip := PrintStartUpInfo()
	delete(nodes, ip)

	node_ips := make([]string, 0, len(nodes))
	for k := range nodes {
		node_ips = append(node_ips, k)
	}

	sm := NewStateMachine(ip)

	handler := NewHTTPHandler(&sm)
	go handler.ListenAndServe(":8080")

	raft_handler := NewRAFTServer(&sm, node_ips)
	raft_handler.ListenAndServe(":6000", node_ips)
}

func PrintStartUpInfo() string {
	host, _ := os.Hostname()
	ips, _ := net.LookupIP(host)
	log.Printf("Hello from %s at %s\n", host, ips[0])

	return ips[0].String()
}
