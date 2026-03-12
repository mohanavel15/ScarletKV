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
	return &Peer{
		ip: ip,
		c:  nil,
	}
}

func (p *Peer) Connect() error {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", p.ip, 6000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	p.c = pb.NewRAFTClient(conn)

	return nil
}

type RAFTServer struct {
	pb.UnimplementedRAFTServer
	sm    *StateMachine
	timer *time.Timer
	mx    sync.Mutex
	peers map[string]*Peer
}

func NewRAFTServer(sm *StateMachine, node_ips []string) *RAFTServer {
	peers := map[string]*Peer{}

	for _, ip := range node_ips {
		peer := NewPeer(ip, 6000)
		peers[ip] = peer
	}

	return &RAFTServer{
		sm:    sm,
		timer: nil,
		peers: peers,
	}
}

func (s *RAFTServer) ResetTimer() {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.timer.Reset(time.Duration(s.sm.timeout) * time.Millisecond)
}

func (s *RAFTServer) HandleTimeout() {
	for {
		<-s.timer.C
		if s.sm.GetState() == LEADER {
			continue
		}

		fmt.Println("Hit The Timout!", s.sm.timeout, "ms")
		s.StartLeaderElection()
	}
}

func (s *RAFTServer) StartHeartBeat() {
	heart_beat_ms := time.Duration(TIME_RATE) * time.Millisecond

	timer := time.NewTimer(heart_beat_ms)

	for {
		if s.sm.GetState() != LEADER {
			timer.Stop()
			break
		}

		for _, peer := range s.peers {
			go func() {
				ctx := context.Background()
				_, err := peer.c.AppendEntries(ctx, &pb.AppendRequest{
					Term:         s.sm.GetTerm(),
					LeaderId:     s.sm.GetId(),
					PrevLogIndex: s.sm.GetLogIndex(),
					PrevLogTerm:  s.sm.GetTerm(),
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

func (s *RAFTServer) StartLeaderElection() {
	log.Println("STARTING AN ELETION")

	s.sm.SetState(CANDIDATE)

	votes := make(chan int, len(s.peers))
	cancelElection := false

	new_term := s.sm.GetTerm() + 1

	go func() {
		wg := sync.WaitGroup{}
		for _, peer := range s.peers {
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_RATE-50)*time.Millisecond)
				defer cancel()

				response, err := peer.c.RequestVote(ctx, &pb.VoteRequest{
					Term:         new_term,
					CandidateId:  s.sm.GetId(),
					LastLogIndex: s.sm.GetLogIndex(),
					LastLogTerm:  s.sm.GetTerm(),
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
					if response.Term > s.sm.GetTerm() {
						cancelElection = true
						s.sm.SetTerm(response.Term)
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
		s.sm.SetState(FOLLOWER)
		s.ResetTimer()
		return
	}

	s.sm.SetState(LEADER)
	s.sm.SetTerm(new_term)
	s.ResetTimer()

	log.Println("I BECOME A LEADER!!!")

	go s.StartHeartBeat()
}

func (s *RAFTServer) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	s.ResetTimer()

	if req.Term <= s.sm.GetTerm() {
		return &pb.VoteResponse{
			Term:        s.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	if req.LastLogIndex < s.sm.GetLogIndex() {
		return &pb.VoteResponse{
			Term:        s.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	s.sm.SetTerm(req.Term)
	s.sm.SetVotedFor(req.CandidateId)
	s.sm.SetState(FOLLOWER)

	return &pb.VoteResponse{
		Term:        s.sm.GetTerm(),
		VoteGranted: true,
	}, nil
}

func (s *RAFTServer) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	s.ResetTimer()

	if req.Term < s.sm.GetTerm() {
		return &pb.AppendResponse{
			Term:    s.sm.GetTerm(),
			Success: false,
		}, nil
	}

	if req.GetPrevLogIndex() < s.sm.GetLogIndex() && req.Term <= s.sm.GetTerm() {
		return &pb.AppendResponse{
			Term:    s.sm.GetTerm(),
			Success: false,
		}, nil
	}

	if req.Term > s.sm.GetTerm() {
		s.sm.SetTerm(req.Term)

		if s.sm.GetState() != FOLLOWER {
			s.sm.SetState(FOLLOWER)
		}
	}

	fmt.Println("FIX ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! server.go:L240")

	// s.sm.IncLogIndex()

	return &pb.AppendResponse{
		Term:    s.sm.GetTerm(),
		Success: true,
	}, nil
}

func (s *RAFTServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRAFTServer(grpcServer, s)

	errChan := make(chan error)

	go func() {
		err := grpcServer.Serve(lis)
		errChan <- err
	}()

	time.Sleep(500 * time.Millisecond)

	for ip, peer := range s.peers {
		err := peer.Connect()
		if err != nil {
			fmt.Println("unable to connection peer: ", ip, err.Error())
		}
	}

	timer := time.NewTimer(time.Duration(s.sm.timeout) * time.Millisecond)
	s.timer = timer

	go s.HandleTimeout()

	return <-errChan
}
