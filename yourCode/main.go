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
	currentLearder int32
	electionTimeout int32 //in milliseconds
	heartBeatInterval int32 //in milliseconds
	//electionChan chan int32
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
	//log.Print(nodeidPortMap)
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		id:                int32(nodeId),
		log:               nil,
		currentTerm:       0,
		votedFor:          -1,
		serverState:       raft.Role_Follower,
		commitIndex:       0,
		kvstore:           nil,
		nextIndex:         nil,
		matchIndex:        nil,
		currentLearder:    -1,
		electionTimeout:   10000,
		heartBeatInterval: 500,
		//electionChan:      make(chan int32),
		electionTimer:	   time.NewTimer(10*time.Second),
		heartBeatTimer:	   time.NewTimer(100*time.Second),
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

	//time.Sleep(time.Duration(electionTimeout))

	go func() {
		for {
			select {
			case t:= <-rn.electionTimer.C:{
				log.Printf("ElectTimer triggered node %d timeout = %d, t = %s",rn.id, rn.electionTimeout, t)
				rn.serverState = raft.Role_Candidate
				rn.voteCounter+=1
				rn.votedFor =rn.id
				for hostId, client := range hostConnectionMap {
					go func(hostId int32, client raft.RaftNodeClient) {
						rva := &raft.RequestVoteArgs{
							From: int32(nodeId),
							To: hostId,
							Term: rn.currentTerm+1,
							CandidateId: rn.id,
							LastLogIndex: rn.commitIndex,
							LastLogTerm: int32(len(rn.log)),
						}
						r, err := client.RequestVote(context.Background(), rva)
						if err != nil {log.Print("err is not nil wor dllm")
						} else {
							if r.VoteGranted ==true{
								//log.Printf("node %d got vote from %d", rn.id, hostId)
								rn.voteCounter+=1
								//log.Printf("node %d voteCounter = %d",rn.id,rn.voteCounter)
								if rn.voteCounter>2{
									rn.serverState = raft.Role_Leader
									log.Printf("node %d is Leader now!!!!!!!!!!!", rn.id)
									rn.currentTerm +=1
									rn.currentLearder =rn.id
									//set heartbeat to reset everyone
									if !rn.heartBeatTimer.Stop() {
										<-rn.heartBeatTimer.C
									}
									rn.heartBeatTimer.Reset(time.Nanosecond)
									rn.resetHeartBeatTimer()
								}
							}
						}
					}(hostId,client)
				}
			}
			case t:=<-rn.heartBeatTimer.C:{
				log.Printf("HeartBeat triggered by node %d, t = %s",rn.id, t)
				if rn.serverState==raft.Role_Follower{
					break
				}
				for hostId, client := range hostConnectionMap {
					go func(hostId int32, client raft.RaftNodeClient) {
						args := &raft.AppendEntriesArgs{
							From:         rn.id,
							To:           hostId,
							Term:         rn.currentTerm,
							LeaderId:     rn.id,
							PrevLogIndex: rn.commitIndex,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: 0,
						}
						r, err := client.AppendEntries(context.Background(), args)
						if err != nil {log.Print("err is not nil wor dllm")
						}else {
							log.Printf("node %d : I got learder heartbeat", r.GetFrom())
						}
					}(hostId,client)
				}
			}

			}
		}
	}()

	log.Printf("NewRaftNode %d returned" , rn.id)
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
	log.Printf("I am raftNode %d I am in Propose()" , rn.id)
	// TODO: Implement this!
	log.Printf("Receive propose from client")
	
	var ret raft.ProposeReply
	if rn.serverState != raft.Role_Leader{
		ret.Status=raft.Status_WrongNode
		ret.CurrentLeader= rn.currentLearder
	} else {
		log := &raft.LogEntry{
			Term:  rn.currentTerm,
			Op:    args.GetOp(),
			Key:   args.GetKey(),
			Value: args.GetV(),
		}
		rn.log = append(rn.log, log)
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
	log.Printf("I am raftNode %d I am in GetValue()", rn.id)
	// TODO: Implement this!
	var ret raft.GetValueReply
	if val, ok := rn.kvstore[args.GetKey()]; ok {
		//do something here
		ret.V = val
		ret.Status = raft.Status_OK
	}else{
		log.Print("rn.GetValue() failed")
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
	// TODO: Implement this!

	var reply raft.RequestVoteReply
	if rn.currentLearder ==-1 {
		rn.currentLearder = args.GetCandidateId()
		rn.serverState = raft.Role_Follower
		rn.votedFor = args.GetCandidateId()
		rn.currentTerm = args.GetTerm()
		reply.VoteGranted = true
		reply.Term = args.Term
		reply.To = args.From
		reply.From = rn.id
	} else {
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

	// TODO: Implement this
	var reply raft.AppendEntriesReply
	rn.resetElectionTimer()
	if args.GetFrom() == rn.currentLearder{
		if args.Term<rn.currentTerm{
			reply.Success = false
		}else {
			reply.Success = true
			reply.From = rn.id
			reply.To = args.GetFrom()
			reply.Term = args.GetTerm()
			reply.MatchIndex = args.GetLeaderCommit()
		}
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
	log.Printf("I am raftNode %d I am in SetHeartBeatInterval()", rn.id)

	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.GetInterval()
	rn.resetHeartBeatTimer()
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	log.Printf("I am raftNode %d I am in CheckEvents()", rn.id)

	return nil, nil
}

func (rn *raftNode) resetElectionTimer() () {
	if !rn.electionTimer.Stop() {
		<-rn.electionTimer.C
	}
	//log.Print("rn.electionTimeout = ", rn.electionTimeout)
	rn.electionTimer.Reset(time.Duration(rn.electionTimeout)*time.Millisecond)
}

func (rn *raftNode) resetHeartBeatTimer() {
	if !rn.heartBeatTimer.Stop() {
		<-rn.heartBeatTimer.C
	}
	rn.heartBeatTimer.Reset(time.Duration(rn.heartBeatInterval)*time.Millisecond)
	log.Print("heart channel getvalue = ", <-rn.heartBeatTimer.C)
}
