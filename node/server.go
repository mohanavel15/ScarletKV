package main

import (
	"context"
	"fmt"
	"log"
	"net"
	pb "node/raft_pb"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	ip string
	c  pb.RAFTClient
}

func NewPeer(ip string, port int) *Peer {
	// Well an interesting observation I had is it doesn't have to connect client? only connects when it needs to send messages?
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", ip, 6000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// What type error can even occur here?
		fmt.Println("What happened here ????", err.Error())
	}

	return &Peer{
		ip: ip,
		c:  pb.NewRAFTClient(conn),
	}
}

type Raft struct {
	pb.UnimplementedRAFTServer
	sm    *StateMachine
	timer *time.Timer
	mx    sync.Mutex
	peers map[string]*Peer
}

func NewRaft(sm *StateMachine, node_ips []string) *Raft {
	peers := map[string]*Peer{}

	for _, ip := range node_ips {
		peer := NewPeer(ip, 6000)
		peers[ip] = peer
	}

	return &Raft{
		sm:    sm,
		timer: nil,
		peers: peers,
	}
}

func (r *Raft) ResetTimer() {
	r.mx.Lock()
	defer r.mx.Unlock()

	r.timer.Reset(time.Duration(r.sm.timeout) * time.Millisecond)
}

func (r *Raft) HandleTimeout() {
	for {
		<-r.timer.C
		if r.sm.GetState() == LEADER {
			continue
		}

		fmt.Println("Hit The Timout!", r.sm.timeout, "ms")
		r.StartLeaderElection()
	}
}

func (r *Raft) StartHeartBeat() {
	heart_beat_ms := time.Duration(TIME_RATE) * time.Millisecond

	timer := time.NewTimer(heart_beat_ms)

	for {
		if r.sm.GetState() != LEADER {
			timer.Stop()
			break
		}

		for _, peer := range r.peers {
			go func() {
				ctx := context.Background()
				_, err := peer.c.AppendEntries(ctx, &pb.AppendRequest{
					Term:         r.sm.GetTerm(),
					LeaderId:     r.sm.GetId(),
					PrevLogIndex: r.sm.GetLogIndex(),
					PrevLogTerm:  r.sm.GetTerm(),
					LeaderCommit: -1,
					Entries:      []*pb.LogEntry{},
				})

				if err != nil {
					log.Printf("PROGRAMMING GODS IDK WHAT TO HERE %v\n", err)
					return
				}
			}()
		}
		<-timer.C
		timer.Reset(heart_beat_ms)
	}
}

func (r *Raft) StartLeaderElection() {
	log.Println("STARTING AN ELETION")

	r.sm.SetState(CANDIDATE)

	votes := make(chan int, len(r.peers))
	cancelElection := false

	new_term := r.sm.GetTerm() + 1

	go func() {
		wg := sync.WaitGroup{}
		for _, peer := range r.peers {
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_RATE-50)*time.Millisecond)
				defer cancel()

				response, err := peer.c.RequestVote(ctx, &pb.VoteRequest{
					Term:         new_term,
					CandidateId:  r.sm.GetId(),
					LastLogIndex: r.sm.GetLogIndex(),
					LastLogTerm:  r.sm.GetTerm(),
				})

				if err != nil {
					log.Printf("PROGRAMMING GODS IDK WHAT TO HERE %v\n", err)
					votes <- 0
					wg.Done()
					return
				}

				if response.VoteGranted {
					votes <- 1
				} else {
					if response.Term > r.sm.GetTerm() {
						cancelElection = true
						r.sm.SetTerm(response.Term)
					} else {
						votes <- -1
					}
				}

				wg.Done()
			}()
		}

		wg.Wait()
		close(votes)
	}()

	voteCount := 1

	for vote := range votes {
		voteCount += vote
	}

	if cancelElection || voteCount <= 0 {
		log.Println("ELECTION LOST / CANCELLED")
		r.sm.SetState(FOLLOWER)
		r.ResetTimer()
		return
	}

	r.sm.SetState(LEADER)
	r.sm.SetTerm(new_term)
	r.ResetTimer()

	log.Println("I BECOME A LEADER!!!")

	go r.StartHeartBeat()
}

func (r *Raft) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	r.ResetTimer()

	if req.Term <= r.sm.GetTerm() {
		return &pb.VoteResponse{
			Term:        r.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	if req.LastLogIndex < r.sm.GetLogIndex() {
		return &pb.VoteResponse{
			Term:        r.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	r.sm.SetTerm(req.Term)
	r.sm.SetVotedFor(req.CandidateId)
	r.sm.SetState(FOLLOWER)

	return &pb.VoteResponse{
		Term:        r.sm.GetTerm(),
		VoteGranted: true,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	r.ResetTimer()

	if req.Term < r.sm.GetTerm() {
		return &pb.AppendResponse{
			Term:    r.sm.GetTerm(),
			Success: false,
		}, nil
	}

	if req.GetPrevLogIndex() < r.sm.GetLogIndex() && req.Term <= r.sm.GetTerm() {
		return &pb.AppendResponse{
			Term:    r.sm.GetTerm(),
			Success: false,
		}, nil
	}

	if req.Term > r.sm.GetTerm() {
		r.sm.SetTerm(req.Term)

		if r.sm.GetState() != FOLLOWER {
			r.sm.SetState(FOLLOWER)
		}
	}

	for _, entry := range req.Entries {
		switch entry.Op {
		case pb.OP_SET:
			fmt.Println("Set")
		case pb.OP_DELETE:
			fmt.Println("Set")
		}

		r.sm.IncLogIndex()
	}

	fmt.Println("FIX ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! server.go:L240")

	return &pb.AppendResponse{
		Term:    r.sm.GetTerm(),
		Success: true,
	}, nil
}

func (r *Raft) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRAFTServer(grpcServer, r)

	errChan := make(chan error)

	go func() {
		err := grpcServer.Serve(lis)
		errChan <- err
	}()

	time.Sleep(500 * time.Millisecond)

	timer := time.NewTimer(time.Duration(r.sm.timeout) * time.Millisecond)
	r.timer = timer

	go r.HandleTimeout()

	return <-errChan
}
