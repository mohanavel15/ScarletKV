package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"node/ptypes"
	"sort"
	"sync"
	"time"

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

// type Peers struct {
// 	peers map[string]*Peer
// }

// func NewPeers(ips []string, port int) Peers {
// 	peers := map[string]*Peer{}

// 	for _, ip := range ips {
// 		peer := NewPeer(ip, port)
// 		peers[ip] = peer
// 	}

// 	return Peers{
// 		peers: peers,
// 	}
// }

// func (p *Peers) Boardcast(fn func(*Peer)) {

// }

type Raft struct {
	ptypes.UnimplementedRaftServer
	ip    string
	port  int
	sm    *StateMachine
	timer *time.Timer
	peers map[string]*Peer

	DistributorC chan *ptypes.LogEntry
	onCommit     func(*ptypes.LogEntry) bool

	server *grpc.Server
}

func NewRaft(ip string, port int, sm *StateMachine, node_ips []string, onCommit func(*ptypes.LogEntry) bool) *Raft {
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
		DistributorC: make(chan *ptypes.LogEntry, len(peers)),
		onCommit:     onCommit,
		server:       grpc.NewServer(),
	}
}

func (r *Raft) ResetTimer() {
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

		r.ReplicateLog()
		<-timer.C
		timer.Reset(heart_beat_ms)
	}
}

func (r *Raft) StartLeaderElection() {
	log.Println("STARTING AN ELETION")

	r.sm.SetState(CANDIDATE)

	votes := make(chan int, len(r.peers))

	current_term := r.sm.GetTerm()
	r.sm.SetTerm(current_term+1, r.sm.GetId())

	go func() {
		wg := sync.WaitGroup{}
		for _, peer := range r.peers {
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_RATE)*time.Millisecond)
				defer cancel()

				response, err := peer.c.RequestVote(ctx, &ptypes.VoteRequest{
					Term:         current_term + 1,
					CandidateId:  r.sm.GetId(),
					LastLogIndex: r.sm.GetLogIndex(),
					LastLogTerm:  r.sm.GetPrevLogTerm(),
				})

				if err == nil && response.VoteGranted {
					votes <- 1
				} else {
					votes <- 0
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

	if voteCount <= (len(r.peers) / 2) {
		log.Println("ELECTION LOST")
		r.sm.SetState(FOLLOWER)
		r.ResetTimer()
		return
	}

	r.sm.SetState(LEADER)
	r.ResetTimer()

	log.Println("I BECOME A LEADER!!!")

	go r.StartHeartBeat()
	go r.DistributeLogEntry()
}

func (r *Raft) DistributeLogEntry() {
	for logEntry := range r.DistributorC {
		if r.sm.GetState() != LEADER {
			continue
		}

		r.sm.LogAppend(logEntry)
		fmt.Println("Distributing logs.........")
		r.ReplicateLog()
	}
}

func (r *Raft) ReplicateLog() {
	for ip, peer := range r.peers {
		go func() {
			for {
				nextIdx, ok := r.sm.NextIndex.Get(ip)
				if !ok {
					r.sm.NextIndex.Set(ip, r.sm.GetLogIndex()+1)
					nextIdx = r.sm.GetLogIndex() + 1
				}

				matchIdx, ok := r.sm.MatchIndex.Get(ip)
				if !ok {
					// TODO: is problematic. this should be -1.
					r.sm.MatchIndex.Set(ip, r.sm.GetLogIndex())
					matchIdx = r.sm.GetLogIndex()
				}

				logs := r.sm.logEntries[nextIdx:] // Probably will trigger come race condition but will deal with it later.

				ctx := context.Background()
				response, err := peer.c.AppendEntries(ctx, &ptypes.AppendRequest{
					Term:         r.sm.GetTerm(),
					LeaderId:     r.sm.GetId(),
					PrevLogIndex: matchIdx, // Actully this is  problematic....
					PrevLogTerm:  r.sm.GetPrevLogTerm(),
					LeaderCommit: r.sm.GetCommitIndex(),
					Entries:      logs,
				})

				if err != nil { // If it errors the then client is offline.
					break // Update during next heart beat
				}

				if response.Success {
					r.sm.MatchIndex.Set(ip, nextIdx+int64(len(logs))-1)
					r.sm.NextIndex.Set(ip, nextIdx+int64(len(logs)))

					r.CheckAndCommit()
					break
				} else {
					// note; nextIdx == 0 && matchIdx == -1 is fine, but i check this because, we don't want to go below this.
					if nextIdx <= 0 {
						log.Println("[nextIdx <= 0] THIS SHOULD NOT BE REACHABLE!")
						break
					}
					r.sm.NextIndex.Set(ip, nextIdx-1)

					if matchIdx <= -1 {
						log.Println("[matchIdx <= -1] THIS SHOULD NOT BE REACHABLE!")
						break
					}
					r.sm.MatchIndex.Set(ip, matchIdx-1)
				}
			}
		}()
	}
}

func (r *Raft) CheckAndCommit() {
	matchIdxs := []int64{r.sm.GetLogIndex()}

	for ip, _ := range r.peers {
		idx, _ := r.sm.MatchIndex.Get(ip)
		matchIdxs = append(matchIdxs, idx)
	}

	sort.Slice(matchIdxs, func(i, j int) bool {
		return matchIdxs[i] > matchIdxs[j]
	})

	majority_idx := matchIdxs[len(matchIdxs)/2]

	if majority_idx > r.sm.GetCommitIndex() {
		fmt.Println("Mojority has replicated so commiting up to index ", majority_idx)
		r.Commit(majority_idx)
	}
}

func (r *Raft) RequestVote(ctx context.Context, req *ptypes.VoteRequest) (*ptypes.VoteResponse, error) {
	if req.Term <= r.sm.GetTerm() {
		return &ptypes.VoteResponse{
			Term:        r.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	// Huhhhhhhhhhhhhhhhhhhhh
	if r.sm.GetTerm() == req.Term && r.sm.GetVotedFor() != "" {
		return &ptypes.VoteResponse{
			Term:        r.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	if req.LastLogIndex < r.sm.GetLogIndex() {
		r.sm.SetTerm(req.Term, "")

		return &ptypes.VoteResponse{
			Term:        r.sm.GetTerm(),
			VoteGranted: false,
		}, nil
	}

	r.ResetTimer()

	r.sm.SetLeader("")
	r.sm.SetTerm(req.Term, req.CandidateId)
	r.sm.SetState(FOLLOWER)

	return &ptypes.VoteResponse{
		Term:        r.sm.GetTerm(),
		VoteGranted: true,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *ptypes.AppendRequest) (*ptypes.AppendResponse, error) {
	r.ResetTimer()

	if req.Term < r.sm.GetTerm() {
		return &ptypes.AppendResponse{
			Term:    r.sm.GetTerm(),
			Success: false,
		}, nil
	} else if req.Term == r.sm.GetTerm() && r.sm.GetState() == LEADER {
		return &ptypes.AppendResponse{
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

	if req.GetPrevLogIndex() > r.sm.GetLogIndex() {
		return &ptypes.AppendResponse{
			Term:    r.sm.GetTerm(),
			Success: false,
		}, nil
	}

	for idx, logE := range req.Entries {
		fmt.Println("Adding entries...")
		r.sm.LogAppendOrInsertAt(req.PrevLogIndex+1+int64(idx), logE)
	}

	if req.LeaderCommit > r.sm.commitIndex {
		fmt.Println("Committing to match leader...")
		r.Commit(req.LeaderCommit)
	}

	return &ptypes.AppendResponse{
		Term:    r.sm.GetTerm(),
		Success: true,
	}, nil
}

// TODO: This whole should do it inside a lock.
func (r *Raft) Commit(logIdx int64) {
	commitIdx := r.sm.GetCommitIndex()

	if logIdx <= commitIdx {
		return
	}

	for i := commitIdx + 1; i < logIdx+1; i++ {
		logEntry := r.sm.logEntries[i]
		if !r.onCommit(logEntry) {
			r.sm.SetCommitIndex(i - 1) // Assuming previous one is success.
			break
		}
	}

	r.sm.SetCommitIndex(logIdx)
}

func (r *Raft) ListenAndServe() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.ip, r.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	ptypes.RegisterRaftServer(r.server, r)

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
