package main

import (
	"context"
	"fmt"
	"log"
	"net"
	pb "node/raft_pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	ip string
	c  *pb.RAFTClient
}

func NewPeer(ip string, port int) (Peer, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", ip, port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Unable to connect to peer %s \n", ip)
		return Peer{}, fmt.Errorf("Failed")
	}

	c := pb.NewRAFTClient(conn)

	return Peer{
		ip: ip,
		c:  &c,
	}, nil
}

type RAFTServer struct {
	pb.UnimplementedRAFTServer
	sm *StateMachine
}

func NewRAFTServer(sm *StateMachine) *RAFTServer {
	return &RAFTServer{
		sm: sm,
	}
}

func (s *RAFTServer) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	if req.LastLogTerm <= s.sm.GetTerm() {
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

	s.sm.SetTerm(req.LastLogTerm)
	s.sm.SetVotedFor(req.CandidateId)

	return &pb.VoteResponse{
		Term:        s.sm.GetTerm(),
		VoteGranted: true,
	}, nil
}

func (s *RAFTServer) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	if req.Term < s.sm.GetTerm() {
		return &pb.AppendResponse{
			Term:    s.sm.GetTerm(),
			Success: false,
		}, nil
	}

	if req.GetPrevLogIndex() < s.sm.GetLogIndex() {
		return &pb.AppendResponse{
			Term:    s.sm.GetTerm(),
			Success: false,
		}, nil
	}

	fmt.Println("FIX ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! server.go:L59")

	s.sm.SetTerm(req.Term)
	s.sm.IncLogIndex()

	return &pb.AppendResponse{
		Term:    s.sm.GetTerm(),
		Success: true,
	}, nil
}

func (s *RAFTServer) ListenAndServe(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRAFTServer(grpcServer, s)

	return grpcServer.Serve(lis)
}
