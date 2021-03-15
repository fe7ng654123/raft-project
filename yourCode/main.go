package main

import (
	"context"
	"csci4160/asgn1/raft"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	id int32
	log []*raft.LogEntry
	// TODO: Implement this!
	currentTerm int32
	votedFor    int32
	serverState raft.Role

	commitIndex int32 //index of highest log entry known to be committed
						// (initialized to 0, increases monotonically)
	kvstore map[string]int32 //[key]value for state machine e.g. A=1; B=2; C=3 , simulate permanent storage

	//Reinitialized after election
	nextIndex map[int32]int32 //for each server, index of the next log entry to send to that server
								// (initialized to leader last log index + 1)
	matchIndex map[int32]int32 //for each server, index of highest log entry known to be replicated on server
								// (initialized to 0, increases monotonically)
	currentLeader     int32
	electionTimeout   int32 //in milliseconds
	heartBeatInterval int32 //in milliseconds
	electionTimer *time.Timer
	heartBeatTimer *time.Timer
	voteCounter int32
}


// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.

func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		id:                int32(nodeId),
		log:               []*raft.LogEntry{},
		currentTerm:       0,
		votedFor:          -1,
		serverState:       raft.Role_Follower,
		commitIndex:       0,
		kvstore:           make(map[string]int32),
		nextIndex:         nil,
		matchIndex:        nil,
		currentLeader:     -1,
		electionTimeout:   int32(electionTimeout),
		heartBeatInterval: int32(heartBeatInterval),
		electionTimer:	   time.NewTimer(time.Duration(electionTimeout) * time.Millisecond),
		heartBeatTimer:	   time.NewTimer(time.Duration(heartBeatInterval) *time.Millisecond),
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	//TODO: kick off leader election here !


	go func() {
		for {
			select {
			case t:= <-rn.electionTimer.C:{
				if rn.serverState == raft.Role_Leader{
					break
				}
				rn.resetElectionTimer()
				log.Printf("ElectTimer triggered node %d timeout = %d, t = %s",rn.id, rn.electionTimeout, t)
				rn.serverState = raft.Role_Candidate
				rn.voteCounter =1
				rn.votedFor = rn.id
				rn.currentTerm +=1
				for hostId, client := range hostConnectionMap {
					go func(hostId int32, client raft.RaftNodeClient) {
						rva := &raft.RequestVoteArgs{
							From: rn.id,
							To: hostId,
							Term: rn.currentTerm,
							CandidateId: rn.id,
							LastLogIndex: int32(len(rn.log)),
							LastLogTerm: 0,
						}
						if int32(len(rn.log))>0{
							rva.LastLogTerm = rn.currentTerm-1
						}
						ctx, cancel := context.WithTimeout(context.Background(),100* time.Millisecond)
						defer cancel()
						r, err := client.RequestVote(ctx, rva)
						//<-ctx.Done()
						if err != nil {
							log.Print("election <-ctx.Done(): ", err)
						}else {
							if r.VoteGranted ==true{
								rn.voteCounter+=1
								log.Printf("node %d voteCounter = %d",rn.id,rn.voteCounter)
								if rn.voteCounter>2 && rn.serverState != raft.Role_Leader{
									rn.serverState = raft.Role_Leader
									log.Printf("node %d is Leader now!!!!!!!!!!!", rn.id)
									//TODO : is it good to init here?
									rn.matchIndex, rn.nextIndex = rn.initIdxforLeader(hostConnectionMap)
									rn.currentLeader=rn.id
									rn.resetHeartBeatTimer(0)
									// revert candidate to follower
									//if rn.voteCounter<3{
									//	rn.serverState = raft.Role_Follower
									//	log.Printf("candidate %d revert to follower", rn.id)
									//}
								}
							}
						}


					}(hostId,client)

				}

			}
			case t:=<-rn.heartBeatTimer.C:{
				if rn.serverState !=raft.Role_Leader{
					break
				}
				rn.resetHeartBeatTimer(rn.heartBeatInterval)
				rn.resetElectionTimer()
				tempCommitIndex := Min(int32(len(rn.log)),rn.commitIndex+1)
				tempCounter:=1
				log.Printf("HeartBeat triggered by node %d, t = %s, state = %s",rn.id, t, rn.serverState)
				for hostId, client := range hostConnectionMap {
					go func(hostId int32, client raft.RaftNodeClient) {
						args := &raft.AppendEntriesArgs{
							From:         rn.id,
							To:           hostId,
							Term:         rn.currentTerm,
							LeaderId:     rn.id,
							Entries:      nil,
							LeaderCommit: rn.commitIndex,
						}

						if len(rn.log)==0 {
							args.PrevLogTerm=0
							args.PrevLogIndex=0
						}else if int32(len(rn.log))>=rn.nextIndex[hostId]{
							if rn.matchIndex[hostId] != 0{
								args.PrevLogTerm = rn.log[rn.matchIndex[hostId]].GetTerm()
							}
							args.PrevLogIndex = rn.matchIndex[hostId]
							args.Entries = rn.log[(rn.nextIndex[hostId]-1):]
							//log.Print("args.Entries ======================", args.Entries)
						}else if int32(len(rn.log))<rn.nextIndex[hostId]{
							if rn.matchIndex[hostId] != 0{
								args.PrevLogTerm = rn.log[rn.matchIndex[hostId]-1].GetTerm()
							}
							args.PrevLogIndex = rn.matchIndex[hostId]
						}

						ctx, cancel := context.WithTimeout(context.Background(),100*time.Millisecond)
						defer cancel()
						r, err := client.AppendEntries(ctx, args)
						//<-ctx.Done()
						if err != nil {
							log.Print("Append <-ctx.Done(): ", err)
							//rn.resetHeartBeatTimer(0)
							//ctx2, cancel2 := context.WithTimeout(context.Background(),100*time.Millisecond)
							//defer cancel2()
							//client.AppendEntries(ctx2, args)
						} else {
							if r.GetSuccess() == false{
								log.Print("false wor dllm!!!!, false node = ",r.GetFrom())
							}
							if r.GetSuccess(){
								log.Print("node " ,r.GetFrom(), ": I report success with matchindex = ", r.GetMatchIndex())
								rn.matchIndex[hostId] = r.GetMatchIndex()
								rn.nextIndex[hostId] = r.GetMatchIndex()+1
								for _,v := range rn.matchIndex{
									if v>=tempCommitIndex{
										tempCounter++
									}
								}
								if tempCounter >2 && r.GetMatchIndex() !=0 && rn.commitIndex != tempCommitIndex{
									rn.commitIndex = tempCommitIndex
									log.Print("Leader replicated logs in majority!!!!! update rn.commitIndex = ", tempCommitIndex)
									LogToCommit := rn.log[tempCommitIndex-1]
									if LogToCommit.GetOp()==raft.Operation_Put{
										rn.kvstore[LogToCommit.GetKey()]=LogToCommit.GetValue()
									} else if LogToCommit.GetOp()==raft.Operation_Delete {
										if _,ok := rn.kvstore[LogToCommit.GetKey()]; ok{
											delete(rn.kvstore,LogToCommit.GetKey())
										}else {
											log.Print("cannot delete key, key not found!!")

										}

									}
									log.Print("node ",rn.id, " kvstore updated = ", rn.kvstore)
									//rn.resetHeartBeatTimer(0)
								}

								//if r.GetMatchIndex() == tempCommitIndex{
								//	tempCounter++
								//	if tempCounter >2 && r.GetMatchIndex() !=0{
								//		rn.commitIndex = tempCommitIndex
								//		log.Print("Leader replicated logs in majority!!!!! update rn.commitIndex = ", tempCommitIndex)
								//
								//		for _,v := range args.GetEntries(){
								//			if v.GetOp()==raft.Operation_Put{
								//				rn.kvstore[v.GetKey()]=v.GetValue()
								//			} else if v.GetOp()==raft.Operation_Delete {
								//				delete(rn.kvstore,v.GetKey())
								//			}
								//			log.Print("kvstore updated = ", rn.kvstore)
								//		}
								//	}
								//}
							}
						}

					}(hostId,client)
				}
			}

			}
		}
	}()
	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	//log.Printf("I am raftNode %d I am in Propose()" , rn.id)
	// TODO: Implement this!

	
	var ret raft.ProposeReply
	if rn.serverState != raft.Role_Leader{
		ret.Status=raft.Status_WrongNode
		ret.CurrentLeader= rn.currentLeader
	} else {
		tempIndex := rn.commitIndex +1
		log.Printf("in node %d ,args key = %s value = %d ops=%s",rn.id, args.GetKey(),args.GetV(), args.GetOp())
		//flag := true
		if args.GetOp() == raft.Operation_Delete{
			if _,ok := rn.kvstore[args.GetKey()]; ok{

			}else {
				log.Print("cannot delete key, key not found!!")
				ret.Status= raft.Status_KeyNotFound
				ret.CurrentLeader = rn.id
				//return &ret, nil
			}
		//	//if !flag{
		//	//	log.Print("cannot add delete log, key not found!!")
		//	//	ret.Status = raft.Status_KeyNotFound
		//	//	ret.CurrentLeader = rn.id
		//	//	return &ret, nil
		//	//}
		}
		logEntry := &raft.LogEntry{
			Term:  rn.currentTerm,
			Op:    args.GetOp(),
			Key:   args.GetKey(),
			Value: args.GetV(),
		}
		rn.log = append(rn.log, logEntry)
		//rn.resetHeartBeatTimer(0)
		log.Print("appended log = ", rn.log, time.Now())
		//for testing
		//rn.kvstore[args.GetKey()]=args.GetV()
		//if !flag{
		//	log.Print("cannot add delete log, key not found!!")
		//	ret.Status = raft.Status_KeyNotFound
		//	ret.CurrentLeader = rn.id
		//	return &ret, nil
		//}

		for rn.commitIndex < tempIndex{
		}
		ret.Status = raft.Status_OK
		ret.CurrentLeader = rn.id

	}
	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	log.Printf("I am raftNode %d I am in GetValue(), my kvstore = %s, time =%s", rn.id, rn.kvstore, time.Now())
	// TODO: Implement this!
	var ret raft.GetValueReply
	//log.Print("-----------",args.GetKey())
	if val, ok := rn.kvstore[args.GetKey()]; ok {
		ret.V = val
		ret.Status = raft.Status_KeyFound
	}else{
		log.Print(rn.id,": rn.GetValue() failed")
		ret.Status = raft.Status_KeyNotFound
	}
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	//log.Printf("Node %d got vote request from %d", rn.id,args.GetFrom())

	rn.resetElectionTimer()
	// TODO: Implement this!
	var reply raft.RequestVoteReply
	if rn.currentTerm ==args.GetTerm() {
		if rn.votedFor == args.CandidateId || rn.votedFor == -1{
			rn.serverState = raft.Role_Follower
			reply.VoteGranted = true
			reply.Term = args.Term
			reply.To = args.From
			reply.From = rn.id
		} else {
			//log.Printf("node %d : same term but i already voted for %d , my currentleader = %d",rn.id,rn.votedFor,rn.currentLeader)
			reply.Term = args.Term
			reply.To = args.From
			reply.From = rn.id
		}
	} else if rn.currentTerm<args.GetTerm(){
		//rn.currentLeader = args.GetCandidateId()
		rn.serverState = raft.Role_Follower
		rn.votedFor = args.GetCandidateId()
		rn.currentTerm = args.GetTerm()
		reply.VoteGranted = true
		reply.Term = args.Term
		reply.To = args.From
		reply.From = rn.id
	} else{
		reply.VoteGranted = false
		reply.From = rn.id
		reply.Term = rn.currentTerm
		reply.To = args.GetFrom()
	}
	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	//log.Printf("I am raftNode %d I am in AppendEntries()", rn.id)
	// TODO: Implement this ; heartbeat can be identified using the 0 length log entry
	var reply raft.AppendEntriesReply

	//ok for now
	rn.resetElectionTimer()
	rn.serverState = raft.Role_Follower

	if args.GetTerm()<rn.currentTerm {
		log.Print("your term term too small la")
		reply.Success = false
		reply.From = rn.id
		reply.To = args.GetFrom()
		reply.Term = args.GetTerm()
	}else if int32(len(rn.log)) < args.GetPrevLogIndex() {
		log.Print("PrevLogIndex NOT matched")
		reply.Success = false
		reply.From = rn.id
		reply.To = args.GetFrom()
		reply.Term = args.GetTerm()
		reply.MatchIndex = args.GetPrevLogIndex()
	}else if args.GetPrevLogIndex()>0 && int32(len(rn.log)) >= args.GetPrevLogIndex() &&
			rn.log[args.GetPrevLogIndex()-1].GetTerm() != args.GetPrevLogTerm() {
		log.Print("PrevLogTerm NOT matched")
		reply.Success = false
		reply.From = rn.id
		reply.To = args.GetFrom()
		reply.Term = args.GetTerm()
		reply.MatchIndex = args.GetPrevLogIndex()
		rn.log = rn.log[0:args.GetPrevLogIndex()]
	}else if args.GetTerm()>=rn.currentTerm{
		rn.currentLeader = args.GetFrom()
		//if args.GetFrom()!=rn.currentLeader {
		//	rn.currentLeader =args.GetFrom()
		//}
		if int32(len(rn.log)) > args.PrevLogIndex{
			rn.log = rn.log[:args.PrevLogIndex]
		}
		rn.log = append(rn.log,args.GetEntries()...)
		log.Printf("node %d's apppened log = %s",rn.id, rn.log)
		rn.currentTerm = args.GetTerm()
		if rn.commitIndex < args.GetLeaderCommit(){
			for _,v := range rn.log[rn.commitIndex:args.GetLeaderCommit()]{
				if v.GetOp()==raft.Operation_Put{
					rn.kvstore[v.GetKey()]=v.GetValue()
				} else if v.GetOp()==raft.Operation_Delete {
					delete(rn.kvstore,v.GetKey())
				}
				log.Print("node ", rn.id," kvstore updated = ", rn.kvstore)
			}
			rn.commitIndex = args.GetLeaderCommit()
		}
		//log.Print("learder's PrevLogIndex = ",args.PrevLogIndex)
		reply.Success = true
		reply.From = rn.id
		reply.To = args.GetFrom()
		reply.Term = args.GetTerm()
		reply.MatchIndex = args.GetPrevLogIndex() +int32(len(args.GetEntries()))
		//reply.MatchIndex = args.GetPrevLogIndex() +1
	}

	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	log.Printf("I am raftNode %d I am in SetElectionTimeout() the timeout = %d", rn.id,args.Timeout)
	// TODO: Implement this!
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeout = args.GetTimeout()
	rn.resetElectionTimer()
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	log.Printf("I am raftNode %d I am in SetHeartBeatInterval(), with heartbeat = %d", rn.id, args.GetInterval())

	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.GetInterval()
	rn.resetHeartBeatTimer(rn.heartBeatInterval)
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	log.Printf("I am raftNode %d I am in CheckEvents()", rn.id)

	return nil, nil
}

func (rn *raftNode) resetElectionTimer() () {
	if !rn.electionTimer.Stop() {
		select{
		case <-rn.electionTimer.C:
			//log.Print("drained")
		default:
			//log.Print("drained default")
		}
	}
	//rn.electionTimer.Stop()
	rn.electionTimer.Reset(time.Duration(rn.electionTimeout)*time.Millisecond)
}

func (rn *raftNode) resetHeartBeatTimer(d int32) {
	if !rn.heartBeatTimer.Stop() {
		select{
		case <-rn.heartBeatTimer.C:
			//log.Print("drained")
		default:
			//log.Print("drained default")
		}
	}
	//rn.heartBeatTimer.Stop()
	rn.heartBeatTimer.Reset(time.Duration(d)*time.Millisecond)
	//log.Print("heart channel getvalue = ", <-rn.heartBeatTimer.C)
}

func (rn *raftNode) initIdxforLeader( hostmap map[int32]raft.RaftNodeClient) (map[int32]int32, map[int32]int32) {
	matchIdex := make(map[int32]int32)
	for hostID, _ := range hostmap{
		matchIdex[hostID]=int32(len(rn.log))
		//log.Print("in initIdxforLeader adding hostID = ", hostID)
	}

	nextIdex := make(map[int32]int32)
	for hostID, _ := range hostmap{
		nextIdex[hostID]= int32(len(rn.log)+1)
		//log.Print("in initIdxforLeader adding nextIdex = ", len(rn.log)+1)
	}
	return matchIdex,nextIdex
}
func Min(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

//func (rn *raftNode) resentAppend(client raft.RaftNodeClient,args *raft.AppendEntriesArgs) (r *raft.AppendEntriesReply,err error ){
//	ctx, cancel := context.WithTimeout(context.Background(),100*time.Millisecond)
//	defer cancel()
//	r, err = client.AppendEntries(ctx, args)
//	go func() {
//		select {
//		case <-ctx.Done():
//			if err != nil {
//				fmt.Println("Append <-ctx.Done(): ", err)
//			}
//			log.Print("let me resentAppend here")
//			rn.resentAppend(client,args)
//		}
//	}()
//	return r, err
//}
