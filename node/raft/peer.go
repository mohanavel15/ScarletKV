package raft

import (
	"fmt"
	"log"
	"node/ptypes"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	ip   string
	c    ptypes.RaftClient
	conn *grpc.ClientConn
}

func NewPeer(ip string, port int) *Peer {
	// Well an interesting observation I had is it doesn't have to connect client? only connects when it needs to send messages?
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", ip, 6000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// What type error can even occur here?
		fmt.Println("What happened here ????", err.Error())
	}

	return &Peer{
		ip:   ip,
		c:    ptypes.NewRaftClient(conn),
		conn: conn,
	}
}

type Peers struct {
	ips   []string
	port  int
	peers map[string]*Peer
	len   int
	mx    sync.RWMutex
}

func NewPeers(ips []string, port int) *Peers {
	peers := map[string]*Peer{}

	for _, ip := range ips {
		peer := NewPeer(ip, port)
		peers[ip] = peer
	}

	return &Peers{
		ips:   ips,
		port:  port,
		peers: peers,
		len:   len(ips),
	}
}

func (p *Peers) Boardcast(fn func(*Peer)) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	wg := sync.WaitGroup{}
	for _, peer := range p.peers {
		wg.Add(1)
		go func() {
			fn(peer)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (p *Peers) Close() {
	p.mx.Lock()
	defer p.mx.Unlock()

	for _, peer := range p.peers {
		err := peer.conn.Close()
		if err != nil {
			log.Printf("Error closing connection %s: %v\n", peer.ip, err)
		}
	}
}
