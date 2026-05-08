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

	scarlet := NewScarlet(ip, node_ips)
	scarlet.Start()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Realistically Never Runs. Or Does it? IDK.

	<-ctx.Done()
	scarlet.Stop()
}

func PrintStartUpInfo() string {
	host, _ := os.Hostname()
	ips, _ := net.LookupIP(host)
	log.Printf("Hello from %s at %s\n", host, ips[0])

	return ips[0].String()
}
