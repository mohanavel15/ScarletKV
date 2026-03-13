package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
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

	man := NewServiceManager()

	raft := NewRaft(ip, 6000, &sm, node_ips)
	man.addService("Raft", raft)

	handler := NewHTTPHandler(ip, 8080, &sm, raft.DistributorC)
	man.addService("RestAPI", handler)

	man.Start()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Realistically Never Runs. Or Does it? IDK.

	<-ctx.Done()
	man.Stop()
}

func PrintStartUpInfo() string {
	host, _ := os.Hostname()
	ips, _ := net.LookupIP(host)
	log.Printf("Hello from %s at %s\n", host, ips[0])

	return ips[0].String()
}
