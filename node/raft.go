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
	ip   string
	c    pb.RAFTClient
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
		c:    pb.NewRAFTClient(conn),
		conn: conn,
	}
}

type Raft struct {
	pb.UnimplementedRAFTServer
	ip    string
	port  int
	sm    *StateMachine
	timer *time.Timer
	mx    sync.Mutex
	peers map[string]*Peer

	DistributorC chan *pb.LogEntry

	server *grpc.Server
}

func NewRaft(ip string, port int, sm *StateMachine, node_ips []string) *Raft {
	peers := map[string]*Peer{}

	for _, peer_ip := range node_ips {
		peer := NewPeer(peer_ip, port)
		peers[peer_ip] = peer
	}

	return &Raft{
		ip:           ip,
		port:         port,
		sm:           sm,
		timer:        nil,
		peers:        peers,
		DistributorC: make(chan *pb.LogEntry, len(peers)),
		server:       grpc.NewServer(),
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

	current_term := r.sm.GetTerm()
	r.sm.SetTerm(current_term+1, r.sm.GetId())

	go func() {
		wg := sync.WaitGroup{}
		for _, peer := range r.peers {
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_RATE)*time.Millisecond)
				defer cancel()

				response, err := peer.c.RequestVote(ctx, &pb.VoteRequest{
					Term:         current_term + 1,
					CandidateId:  r.sm.GetId(),
					LastLogIndex: r.sm.GetLogIndex(),
					LastLogTerm:  current_term,
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
					// if response.Term > r.sm.GetTerm() {
					// 	cancelElection = true
					// 	r.sm.SetTerm(response.Term, "")
					// } else {
					// 	votes <- -1
					// }

					votes <- -1
				}

				wg.Done()
			}()
		}

		wg.Wait()
		close(votes)
	}()

	voteCount := 1 // One for self-vote

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
	r.ResetTimer()

	log.Println("I BECOME A LEADER!!!")

	go r.StartHeartBeat()
}

func (r *Raft) DistributeLogEntry() {
	for logEntry := range r.DistributorC {
		if r.sm.GetState() != LEADER {
			continue
		}

		r.sm.LogAppend(logEntry)
		r.ReplicateLog()
	}
}

func (r *Raft) ReplicateLog() {
	for ip, peer := range r.peers {
		go func() {
			retryConn := 0

			for retryConn < 3 {
				nextIdx, ok := r.sm.NextIndex.Get(ip)
				if !ok {
					r.sm.NextIndex.Set(ip, 0)
					nextIdx = 0
				}

				matchIdx, ok := r.sm.MatchIndex.Get(ip)
				if !ok {
					r.sm.MatchIndex.Set(ip, -1)
					matchIdx = -1
				}

				logs := r.sm.logEntries[nextIdx:] // Probably will trigger come race condition but will deal with it later.

				ctx := context.Background()
				response, err := peer.c.AppendEntries(ctx, &pb.AppendRequest{
					Term:         r.sm.GetTerm(),
					LeaderId:     r.sm.GetId(),
					PrevLogIndex: matchIdx,
					PrevLogTerm:  r.sm.GetTerm(), // Huhhhhhhhhhhhhhhhhhhh what do I do here?
					LeaderCommit: -1,             // Will come back to this.
					Entries:      logs,
				})

				if err != nil { // If it errors the then client is offline.
					retryConn += 1
					log.Printf("[%s] Connection Error will Retry Again...\n", ip)
					continue
				}

				if response.Success {
					r.sm.MatchIndex.Set(ip, nextIdx) // I need to check when to read Next Index, RN only writing.
					r.sm.NextIndex.Set(ip, nextIdx+int64(len(logs)))
					break
				} else {
					if nextIdx <= 0 {
						log.Println("[nextIdx <= 0] THIS SHOULD NOT BE REACHABLE!")
					}

					r.sm.NextIndex.Set(ip, nextIdx-1)
				}
			}
		}()
	}
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
		r.sm.SetTerm(req.Term, "")

		// r.StartLeaderElection() Not sure if this is what I'm supposed to do.

		return &pb.VoteResponse{
			Term:        r.sm.GetTerm(),
			VoteGranted: false,
		}, nil

	}

	r.sm.SetLeader("")
	r.sm.SetTerm(req.Term, req.CandidateId)
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
	} else if req.Term == r.sm.GetTerm() && r.sm.GetState() == LEADER {
		return &pb.AppendResponse{
			Term:    r.sm.GetTerm(),
			Success: false,
		}, nil
	} else {
		r.sm.SetTerm(req.Term, "")

		r.sm.SetLeader(req.LeaderId)

		if r.sm.GetState() != FOLLOWER {
			r.sm.SetState(FOLLOWER)
		}
	}

	// Not sure about this part..... Will come to this later....
	if req.GetPrevLogIndex() > r.sm.GetLogIndex() {
		return &pb.AppendResponse{
			Term:    r.sm.GetTerm(),
			Success: false,
		}, nil
	}

	for idx, logE := range req.Entries {
		r.sm.LogAppendOrInsertAt(req.PrevLogIndex+1+int64(idx), logE)
	}

	r.sm.SetLogIndex(req.PrevLogIndex + int64(len(req.Entries)))

	if req.LeaderCommit > r.sm.commitIndex {
		r.Commit(req.LeaderCommit)
	}

	return &pb.AppendResponse{
		Term:    r.sm.GetTerm(),
		Success: true,
	}, nil
}

func (r *Raft) Commit(logIdx int64) {
	for i := r.sm.commitIndex + 1; i < logIdx+1; i++ {

	}

}

func (r *Raft) ListenAndServe() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.ip, r.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	pb.RegisterRAFTServer(r.server, r)

	errChan := make(chan error)

	go func() {
		err := r.server.Serve(lis)
		errChan <- err
	}()

	time.Sleep(time.Duration(TIME_RATE*2) * time.Millisecond)

	timer := time.NewTimer(time.Duration(r.sm.timeout) * time.Millisecond)
	r.timer = timer

	go r.HandleTimeout()

	return <-errChan
}

func (r *Raft) Close() {
	for _, peer := range r.peers {
		err := peer.conn.Close()
		if err != nil {
			log.Printf("Error closing connection %s: %v\n", peer.ip, err)
		}
	}

	r.server.GracefulStop()
}
