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

	raft := NewRaft(&sm, node_ips)
	go raft.ListenAndServe(":6000")

	handler := NewHTTPHandler(&sm, raft.DistributorC)
	handler.ListenAndServe(":8080")
}

func PrintStartUpInfo() string {
	host, _ := os.Hostname()
	ips, _ := net.LookupIP(host)
	log.Printf("Hello from %s at %s\n", host, ips[0])

	return ips[0].String()
}
