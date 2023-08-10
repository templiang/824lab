package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// Status 节点角色
type Status int

// VoteState 投票的的状态
type VoteState int

// AppendEntriesState 追加日志的状态
type AppendEntriesState int

// 节点类型
const (
	Follower = iota
	Candidate
	Leader
)

// 投票状态，正常、节点已中止、投票消息过期、本 Term 已投过
const (
	Normal = iota
	Killed
	Expire
	voted
)

const (
	AppNormal    = iota // 追加正常
	AppOutOfDate        // 追加过时
	Appkilled           // Raft 程序终止
	//AppRepeat           // 追加重复
	//AppCommited         //
	//Mismatch
)

const (
	HeartbeatDuration = 30
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// server基础成员
	currentTerm int //当前任期
	votedFor    int
	logs        []LogEntry

	// 记录日志提交和应用的变量
	commitIndex int // 本节点已知已经被提交的最后一个日志条目索引值
	lastApplied int // 已经应用到状态机的最后一个日志条目的索引值

	// leader 通过以下变量管理 follower
	nextIndex  []int // nextIndex[i] 记录发送到该服务器的下一个日志条目的索引
	matchIndex []int // matchIndex[i] 记录

	status           Status        // 当前节点的角色
	electionDuration time.Duration // 超时时间
	electionTimer    *time.Ticker  // 选举定时器
	heartbeatTimer   *time.Timer
}

// AppendEntriesArgs 按照 figure2 创建
type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // leader id
	PrevLogIndex int        // 最新日志条目的前一个日志条目的索引号
	PreLogTerm   int        // 最新日志条目的前一个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目，可以是多个，发送心跳时为空
	LeaderCommit int        // leader 已知已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func randomElectionDuration(id int) time.Duration {
	rand.Seed(int64(id) * time.Now().UnixNano())                // 初始化随机数种子
	return time.Duration(150+rand.Intn(200)) * time.Millisecond //随机生成 150 - 350 ms的延迟
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人需要竞选的任期
	CandidateId  int //候选人ID
	LastLogIndex int //候选人最后日志条目索引
	LastLogTerm  int //候选人最后日志任期
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 当候选人的任期小于本节点的任期，不投票，返回自己的最新日志条目的任期
// 当本节点 VotedFor 为空（目前还有票），且候选人的日志条目跟当前节点的一样新，则投票之
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VoteState   VoteState
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("当前结点：", rf.status, " ", rf.currentTerm)
	//fmt.Printf("候选人结点：%+v\n", *args)
	// 不可投票
	if args.Term < rf.currentTerm {
		// 此时候选人不可被选为 Leader
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 可以投票，更新当前节点信息
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	// 竞争票的情况，如果在当前任期已经为其它候选者投过票，拒绝投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 不予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 投票，重置定时器
	rf.electionTimer.Reset(randomElectionDuration(rf.me))
	rf.votedFor = args.CandidateId

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// select语句会包含多个case分支，用于监听多个通道的数据，并根据接收到数据的情况执行相应的操作。当select内没有default分支时，监听模式为阻塞监听，否则，当所有通道都没准备好时，执行default分支
		select {
		// rf.timer.C是一个类型为<-chan Time的通道，当定时器到达指定时间时，该通道会有数据可读，执行case <-rf.timer.C这个分支的代码块
		case <-rf.electionTimer.C:
			rf.mu.Lock()

			if rf.status == Leader {
				rf.mu.Unlock()
				break
			}

			rf.status = Candidate
			rf.mu.Unlock()
			fmt.Println("准备投票")
			go rf.startElection()
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// 候选者调用此函数发起投票
func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer.Reset(randomElectionDuration(rf.me))

	rf.mu.Unlock()

	var args RequestVoteArgs

	rf.mu.Lock()

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	//args.LastLogTerm = rf.logs[args.LastLogIndex].Term

	rf.mu.Unlock()

	fmt.Printf("id:%d 成为候选者，发起请求投票流程\n", args.CandidateId)
	voteChanel := make(chan bool, len(rf.peers)-1)

	for i := range rf.peers {
		// 跳过自己
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			// 请求投票失败，我们将其记录为未获取选票
			if rf.sendRequestVote(i, &args, &reply) == false {
				voteChanel <- false
				return
			}

			rf.mu.Lock()
			// 投票者的任期比候选人还更加新，候选人不可成为 leader ,修改本节电的状态为 Follower，更新本节点信息，退出竞选
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = Follower
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			// 请求投票成功，我们判断是否获得了选票
			voteChanel <- reply.VoteGranted
		}(i)
	}
	grantedCnt := 1
	// grantedResult := false
	cnt := 1
	for voteGranted := range voteChanel {
		fmt.Printf("第%d次循环\n", cnt)
		cnt++
		rf.mu.Lock()
		// 如果节点因为某些原因已不再是候选人，退出计票过程
		if rf.status != Candidate {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if voteGranted {
			grantedCnt++
		}

		if grantedCnt > len(rf.peers)/2 {
			// 获取了超过半数的选票
			// grantedResult = true
			fmt.Printf("id:%+v ,成为leader:\n", args)
			rf.mu.Lock()
			rf.status = Leader
			rf.mu.Unlock()
			go rf.heartbeat()
			break
		}
		if cnt == len(rf.peers) {
			// 当计票完成后，我们需要主动退出循环，否则会阻塞
			fmt.Println("计票完成")
			return
		}
	}
	fmt.Printf("id:%+v ,获取选票数:%d\n", args, grantedCnt)
	//if grantedResult {
	//	// 获取了超过半数选票,更新状态为 Leader，并开始向各节点发送心跳
	//	fmt.Printf("id:%d ,成为leader:\n", args.CandidateId)
	//	rf.mu.Lock()
	//	rf.status = Leader
	//	rf.mu.Unlock()
	//
	//	go rf.heartbeat()
	//}

}

func (rf *Raft) heartbeat() {
	heartChPool := make([]chan struct{}, len(rf.peers))
	heartDoneChPool := make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		heartChPool[i] = make(chan struct{})
		heartDoneChPool[i] = make(chan struct{})

		// 使用协程向各个节点发送心跳
		go func(i int) {
			for {
				select {
				case <-heartChPool[i]:
					args := AppendEntriesArgs{}
					rf.mu.Lock()
					args.LeaderId = rf.me
					args.Term = rf.currentTerm
					rf.mu.Unlock()

					reply := AppendEntriesReply{}
					go func() {
						if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
							return
						}

						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.status = Follower
							//rf.mu.Unlock()
							//return
						}
						rf.mu.Unlock()
					}()

				case <-heartDoneChPool[i]:
					return
				}
			}
		}(i)
	}

	broadcast := func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				heartChPool[i] <- struct{}{}
			}(i)
		}
	}
	broadcast()

	rf.heartbeatTimer = time.NewTimer(HeartbeatDuration * time.Millisecond)
	//rf.heartbeatTimer.Reset()
	for {
		<-rf.heartbeatTimer.C
		rf.mu.Lock()
		isLeader := rf.status == Leader
		rf.mu.Unlock()

		if rf.killed() || !isLeader {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					heartDoneChPool[i] <- struct{}{}
				}(i)
			}
			break
		}

		rf.heartbeatTimer.Reset(HeartbeatDuration * time.Millisecond)
		rf.electionTimer.Reset(randomElectionDuration(rf.me))
		broadcast()
	}
}
func (rf *Raft) replicator() {

}

//func (rf *Raft) newheartbeat() {
//	var syncMu sync.Mutex
//	cond := sync.NewCond(&syncMu)
//
//	broadcast := func() {
//		syncMu.Lock()
//		defer syncMu.Unlock()
//		cond.Broadcast()
//	}
//
//	rf.heartbeatTimer = time.NewTicker(HeartbeatDuration * time.Millisecond)
//
//	for {
//		<-rf.heartbeatTimer.C
//		if rf.killed() || rf.status != Leader {
//			rf.heartbeatTimer.Stop()
//			break
//		}
//
//		broadcast()
//	}
//
//	cond.Broadcast()
//}

//func replicator(i int, rf *Raft, cond *sync.Cond) {
//	for {
//		cond.L.Lock()
//
//	}
//}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.electionDuration = randomElectionDuration(rf.me) //随机生成 150 - 350 ms的延迟
	rf.electionTimer = time.NewTicker(rf.electionDuration)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("id:", rf.me, " 接收到心跳信号:", args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(randomElectionDuration(rf.me))
}
func (rf *Raft) sendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	return ok
}

/////////////////////////

//func (rf *Raft) heartbeat() {
//	rf.heartbeatCond = sync.NewCond(&rf.mu)
//	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_INTERVAL)
//	defer func() {
//		rf.heartbeatTimer.Stop()
//		rf.mu.Lock()
//		rf.heartbeatCond.Broadcast()
//		rf.mu.Unlock()
//	}()
//
//	// 创建一定数量的 worker goroutines
//	numWorkers := 5 // 可根据实际情况调整
//	workerCh := make(chan int, numWorkers)
//	for i := 0; i < numWorkers; i++ {
//		go rf.worker(workerCh)
//	}
//
//	for {
//		select {
//		case <-rf.heartbeatTimer.C:
//			rf.mu.RLock()
//			isLeader := rf.isLeader()
//			rf.mu.RUnlock()
//
//			if rf.killed() || !isLeader {
//				return
//			}
//
//			rf.mu.Lock()
//			rf.heartbeatCond.Broadcast()
//			rf.mu.Unlock()
//
//			rf.heartbeatTimer.Reset(HEARTBEAT_INTERVAL)
//
//		case <-rf.heartbeatCond.Wait():
//			rf.mu.RLock()
//			isLeader := rf.isLeader()
//			rf.mu.RUnlock()
//
//			if rf.killed() || !isLeader {
//				return
//			}
//
//			// 向 worker goroutines 分发任务
//			for i := range rf.peers {
//				if i == rf.me {
//					continue
//				}
//				workerCh <- i
//			}
//		}
//	}
//}
//
//func (rf *Raft) worker(workerCh <-chan int) {
//	for {
//		select {
//		case i := <-workerCh:
//			rf.replicator(i)
//		}
//	}
//}

//func (rf *Raft) replicator(i int) {
//	args := AppendEntriesArgs{LeaderId: rf.me}
//	reply := AppendEntriesReply{}
//
//	// 获取当前节点的 Term，并准备发送心跳消息
//	rf.mu.RLock()
//	args.Term = rf.currentTerm
//	rf.mu.RUnlock()
//
//	// 发送心跳消息并获取回复
//	if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
//		return
//	}
//
//	// 处理心跳回复
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	// 如果回复的 Term 大于当前节点的 Term，则更新 Term 并变成 Follower
//	if reply.Term > rf.currentTerm {
//		rf.currentTerm = reply.Term
//		rf.votedFor = -1
//		rf.state = FOLLOWER
//		return
//	}
//}
