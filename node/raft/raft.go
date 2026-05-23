package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"node/ptypes"
	"node/utils"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
)

var TIME_RATE int64 = 150

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type Raft struct {
	ptypes.UnimplementedRaftServer
	server *grpc.Server

	ip   string
	port int

	leaderIP string
	peers    *Peers

	timeout int64
	timer   *time.Timer

	DistributorC chan *Message
	onCommit     func(*ptypes.LogEntry) bool

	mx sync.RWMutex

	// General States
	term       int64
	logIndex   int64
	votedFor   string
	state      State
	logEntries []*ptypes.LogEntry

	// Volatile States
	commitIndex int64
	lastApplied int64

	// Volatile Leader States
	nextIndex   utils.SyncMap[string, int64]
	matchIndex  utils.SyncMap[string, int64]
	pendingMsgs utils.SyncMap[int64, chan bool]
}

func NewRaft(ip string, port int, node_ips []string, onCommit func(*ptypes.LogEntry) bool) *Raft {
	return &Raft{
		ip:           ip,
		port:         port,
		timeout:      rand.Int64N(TIME_RATE) + TIME_RATE,
		timer:        nil,
		leaderIP:     "",
		peers:        NewPeers(node_ips, port),
		DistributorC: make(chan *Message),
		onCommit:     onCommit,
		server:       grpc.NewServer(),

		// General States
		term:       0,
		logIndex:   -1,
		votedFor:   "",
		state:      FOLLOWER,
		logEntries: []*ptypes.LogEntry{},

		// Volatile States
		commitIndex: -1,
		lastApplied: -1,

		// Volatile Leader States
		nextIndex:   utils.NewSyncMap[string, int64](),
		matchIndex:  utils.NewSyncMap[string, int64](),
		pendingMsgs: utils.NewSyncMap[int64, chan bool](),
	}
}

func (r *Raft) GetState() State {
	r.mx.RLock()
	defer r.mx.RUnlock()

	return r.state
}

func (r *Raft) GetLeader() string {
	r.mx.RLock()
	defer r.mx.RUnlock()

	return r.leaderIP
}

func (r *Raft) ResetTimer() {
	if r.timer != nil {
		r.timer.Reset(time.Duration(r.timeout) * time.Millisecond)
	}
}

func (r *Raft) HandleTimeout() {
	for {
		<-r.timer.C
		r.mx.RLock()
		state := r.state
		r.mx.RUnlock()

		if state == LEADER {
			continue
		}

		fmt.Println("Hit The Timout!", r.timeout, "ms")
		r.StartLeaderElection()
	}
}

func (r *Raft) StartHeartBeat() {
	heart_beat_ms := time.Duration(TIME_RATE) * time.Millisecond
	timer := time.NewTimer(heart_beat_ms)

	for {
		r.mx.RLock()
		state := r.state
		r.mx.RUnlock()

		if state != LEADER {
			timer.Stop()
			break
		}

		r.ReplicateLog()
		<-timer.C
		timer.Reset(heart_beat_ms)
	}
}

func (r *Raft) StartLeaderElection() {
	r.mx.Lock()
	defer r.mx.Unlock()

	log.Println("STARTING AN ELETION")

	r.state = CANDIDATE

	votes := make(chan int, r.peers.len)

	r.term += 1
	r.votedFor = r.ip

	term := r.term
	candidateId := r.ip
	lastLogIndex := r.logIndex
	lastLogTerm := int64(-1)

	if lastLogIndex >= 0 {
		lastLogTerm = r.logEntries[lastLogIndex].Term
	}

	go func() {
		r.peers.Boardcast(func(peer *Peer) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_RATE)*time.Millisecond)
			defer cancel()

			response, err := peer.c.RequestVote(ctx, &ptypes.VoteRequest{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
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
		r.state = FOLLOWER
		r.ResetTimer()
		return
	}

	r.state = LEADER
	r.ResetTimer()

	log.Println("I BECOME A LEADER!!!")

	go r.StartHeartBeat()
	go r.DistributeLogEntry()
}

func (r *Raft) DistributeLogEntry() {
	for msg := range r.DistributorC {
		r.mx.Lock()
		if r.state == LEADER {
			r.logEntries = append(r.logEntries, msg.log)
			r.logIndex += 1
			r.pendingMsgs.Set(r.logIndex, msg.success)
			r.mx.Unlock()
			fmt.Println("Distributing logs.........")
			r.ReplicateLog()
		} else {
			r.mx.Unlock()
			msg.success <- false
			close(msg.success)
		}
	}
}

func (r *Raft) ReplicateLog() {
	r.mx.RLock()
	ip := r.ip
	currentTerm := r.term
	commitIndex := r.commitIndex
	logIndex := r.logIndex
	r.mx.RUnlock()

	r.peers.Boardcast(func(peer *Peer) {
		for {
			nextIdx, ok := r.nextIndex.Get(peer.ip)
			if !ok {
				nextIdx = logIndex + 1
				r.nextIndex.Set(peer.ip, nextIdx)
			}

			r.mx.RLock()
			if r.state != LEADER || r.term != currentTerm {
				r.mx.RUnlock()
				return
			}

			if nextIdx > int64(len(r.logEntries)) {
				nextIdx = int64(len(r.logEntries))
			}

			logs := make([]*ptypes.LogEntry, int64(len(r.logEntries))-nextIdx)
			copy(logs, r.logEntries[nextIdx:])

			previousLogTerm := int64(-1)
			if nextIdx > 0 && nextIdx-1 < int64(len(r.logEntries)) {
				previousLogTerm = r.logEntries[nextIdx-1].Term
			}
			r.mx.RUnlock()

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_RATE)*time.Millisecond)
			response, err := peer.c.AppendEntries(ctx, &ptypes.AppendRequest{
				Term:         currentTerm,
				LeaderId:     ip,
				PrevLogIndex: nextIdx - 1,
				PrevLogTerm:  previousLogTerm,
				LeaderCommit: commitIndex,
				Entries:      logs,
			})

			cancel()
			if err != nil {
				// If it errors the then client is offline.
				// Break, Update during next heart beat
				break
			}

			if response.Success {
				r.matchIndex.Set(peer.ip, nextIdx+int64(len(logs))-1)
				r.nextIndex.Set(peer.ip, nextIdx+int64(len(logs)))
				break
			} else {
				if response.Term > currentTerm {
					r.mx.Lock()
					if response.Term > r.term {
						r.term = response.Term
						r.state = FOLLOWER
						r.votedFor = ""
						r.ResetTimer()
					}
					r.mx.Unlock()
					return
				}

				if nextIdx <= 0 {
					log.Println("[nextIdx <= 0] THIS SHOULD NOT BE REACHABLE!")
					break
				}

				r.nextIndex.Set(peer.ip, nextIdx-1)
			}
		}
	})

	r.CheckAndCommit()
}

func (r *Raft) CheckAndCommit() {
	r.mx.Lock()
	defer r.mx.Unlock()

	matchIdxs := []int64{r.logIndex}

	for _, ip := range r.peers.ips {
		idx, _ := r.matchIndex.Get(ip)
		matchIdxs = append(matchIdxs, idx)
	}

	sort.Slice(matchIdxs, func(i, j int) bool {
		return matchIdxs[i] > matchIdxs[j]
	})

	majority_idx := matchIdxs[len(matchIdxs)/2]

	if majority_idx > r.commitIndex {
		fmt.Println("Mojority has replicated so commiting up to index ", majority_idx)
		r.Commit(majority_idx)
	}
}

func (r *Raft) RequestVote(ctx context.Context, req *ptypes.VoteRequest) (*ptypes.VoteResponse, error) {
	r.mx.Lock()
	defer r.mx.Unlock()

	if req.Term < r.term {
		return &ptypes.VoteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, nil
	}

	if req.Term > r.term {
		r.term = req.Term
		r.votedFor = ""
		r.state = FOLLOWER
		r.leaderIP = ""
	}

	if r.votedFor != "" && r.votedFor != req.CandidateId {
		return &ptypes.VoteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, nil
	}

	lastLogTerm := int64(-1)
	if r.logIndex >= 0 {
		lastLogTerm = r.logEntries[r.logIndex].Term
	}

	if req.LastLogTerm < lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex < r.logIndex) {
		return &ptypes.VoteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, nil
	}

	r.votedFor = req.CandidateId
	r.ResetTimer()

	return &ptypes.VoteResponse{
		Term:        r.term,
		VoteGranted: true,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *ptypes.AppendRequest) (*ptypes.AppendResponse, error) {
	r.mx.Lock()
	defer r.mx.Unlock()

	if req.Term < r.term {
		return &ptypes.AppendResponse{
			Term:    r.term,
			Success: false,
		}, nil
	}

	if req.Term > r.term || r.state != FOLLOWER {
		r.term = req.Term
		r.state = FOLLOWER
		r.votedFor = ""
	}

	r.leaderIP = req.LeaderId
	r.ResetTimer()

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(r.logEntries)) || r.logEntries[req.PrevLogIndex].Term != req.PrevLogTerm {
			return &ptypes.AppendResponse{
				Term:    r.term,
				Success: false,
			}, nil
		}
	}

	for idx, logE := range req.Entries {
		fmt.Println("Adding entries...")

		idx_calc := req.PrevLogIndex + 1 + int64(idx)

		if idx_calc < int64(len(r.logEntries)) {
			r.logEntries = r.logEntries[:idx_calc]
			r.logEntries = append(r.logEntries, logE)
		} else {
			r.logEntries = append(r.logEntries, logE)
		}
	}

	r.logIndex = int64(len(r.logEntries) - 1)

	if req.LeaderCommit > r.commitIndex {
		fmt.Println("Committing to match leader...")
		targetCommit := req.LeaderCommit
		if r.logIndex < targetCommit {
			targetCommit = r.logIndex
		}
		r.Commit(targetCommit)
	}

	return &ptypes.AppendResponse{
		Term:    r.term,
		Success: true,
	}, nil
}

func (r *Raft) Commit(logIdx int64) {
	if logIdx <= r.commitIndex {
		return
	}

	for i := r.commitIndex + 1; i < logIdx+1; i++ {
		logEntry := r.logEntries[i]
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

		r.commitIndex = logIdx
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
	timer := time.NewTimer(time.Duration(r.timeout) * time.Millisecond)
	r.timer = timer
	r.mx.Unlock()

	go r.HandleTimeout()

	return <-errChan
}

func (r *Raft) Close() {
	r.peers.Close()
	r.server.GracefulStop()
}
