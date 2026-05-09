package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"node/ptypes"
	"node/utils"
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

type Raft struct {
	ptypes.UnimplementedRaftServer
	ip    string
	port  int
	sm    *StateMachine
	timer *time.Timer
	peers *Peers

	DistributorC chan *Message
	onCommit     func(*ptypes.LogEntry) bool

	pendingMsgs utils.SyncMap[int64, chan bool]
	server      *grpc.Server

	mx sync.Mutex
}

func NewRaft(ip string, port int, node_ips []string, onCommit func(*ptypes.LogEntry) bool) *Raft {
	sm := NewStateMachine(ip)

	return &Raft{
		ip:           ip,
		port:         port,
		sm:           sm,
		timer:        nil,
		peers:        NewPeers(node_ips, port),
		DistributorC: make(chan *Message),
		pendingMsgs:  utils.NewSyncMap[int64, chan bool](),
		onCommit:     onCommit,
		server:       grpc.NewServer(),
	}
}

// TEMP FIX for now....
func (r *Raft) SM() *StateMachine {
	return r.sm
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

		r.ReplicateLog()
		<-timer.C
		timer.Reset(heart_beat_ms)
	}
}

func (r *Raft) StartLeaderElection() {
	log.Println("STARTING AN ELETION")

	r.sm.SetState(CANDIDATE)

	votes := make(chan int, r.peers.len)

	current_term := r.sm.GetTerm()
	r.sm.SetTerm(current_term+1, r.sm.GetId())

	go func() {
		r.peers.Boardcast(func(peer *Peer) {
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
		})
		close(votes)
	}()

	voteCount := 1 // One for self-vote

	for vote := range votes {
		voteCount += vote
	}

	if voteCount <= (r.peers.len / 2) {
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
	for msg := range r.DistributorC {
		if r.sm.GetState() != LEADER {
			msg.success <- false
			close(msg.success)
			continue
		}

		idx := r.sm.LogAppend(msg.log)
		r.pendingMsgs.Set(idx, msg.success)
		fmt.Println("Distributing logs.........")
		r.ReplicateLog()
	}
}

func (r *Raft) ReplicateLog() {
	r.peers.Boardcast(func(peer *Peer) {
		for {
			nextIdx, ok := r.sm.NextIndex.Get(peer.ip)
			if !ok {
				r.sm.NextIndex.Set(peer.ip, r.sm.GetLogIndex()+1)
				nextIdx = r.sm.GetLogIndex() + 1
			}

			matchIdx, ok := r.sm.MatchIndex.Get(peer.ip)
			if !ok {
				// TODO: is problematic. this should be -1.
				r.sm.MatchIndex.Set(peer.ip, r.sm.GetLogIndex())
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
				r.sm.MatchIndex.Set(peer.ip, nextIdx+int64(len(logs))-1)
				r.sm.NextIndex.Set(peer.ip, nextIdx+int64(len(logs)))

				r.CheckAndCommit()
				break
			} else {
				// note; nextIdx == 0 && matchIdx == -1 is fine, but i check this because, we don't want to go below this.
				if nextIdx <= 0 {
					log.Println("[nextIdx <= 0] THIS SHOULD NOT BE REACHABLE!")
					break
				}
				r.sm.NextIndex.Set(peer.ip, nextIdx-1)

				if matchIdx <= -1 {
					log.Println("[matchIdx <= -1] THIS SHOULD NOT BE REACHABLE!")
					break
				}
				r.sm.MatchIndex.Set(peer.ip, matchIdx-1)
			}
		}
	})
}

func (r *Raft) CheckAndCommit() {
	matchIdxs := []int64{r.sm.GetLogIndex()}

	for _, ip := range r.peers.ips {
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
	r.mx.Lock()
	defer r.mx.Unlock()

	commitIdx := r.sm.GetCommitIndex()

	if logIdx <= commitIdx {
		return
	}

	for i := commitIdx + 1; i < logIdx+1; i++ {
		logEntry := r.sm.logEntries[i]
		if time.Now().UnixMilli() > logEntry.Deadline {
			if ch, ok := r.pendingMsgs.Get(i); ok {
				close(ch)
				r.pendingMsgs.Delete(i)
			}
		} else if r.onCommit(logEntry) {
			if ch, ok := r.pendingMsgs.Get(i); ok {
				ch <- true
				close(ch)
				r.pendingMsgs.Delete(i)
			}
		} else {
			break
		}

		r.sm.SetCommitIndex(logIdx)
	}
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

	r.mx.Lock()
	timer := time.NewTimer(time.Duration(r.sm.timeout) * time.Millisecond)
	r.timer = timer
	r.mx.Unlock()

	go r.HandleTimeout()

	return <-errChan
}

func (r *Raft) Close() {
	r.peers.Close()
	r.server.GracefulStop()
}

type Message struct {
	log         *ptypes.LogEntry
	success     chan bool
	hasTimedOut bool

	mx sync.Mutex
}

func NewMessage(op ptypes.Op, key string, val *ptypes.Value) *Message {
	return &Message{
		log: &ptypes.LogEntry{
			Op:       op,
			Key:      key,
			Value:    val,
			Deadline: time.Now().Add(10 * time.Second).UnixMilli(), // Wait for 10 seconds
		},
		success:     make(chan bool, 1),
		hasTimedOut: false,
	}
}

func (m *Message) WaitForConfirmation() bool {
	timer := time.NewTimer(time.Duration(m.log.Deadline-time.Now().UnixMilli()) * time.Millisecond)
	defer timer.Stop()

	select {
	case val := <-m.success:
		return val
	case <-timer.C:
		return false
	}
}
