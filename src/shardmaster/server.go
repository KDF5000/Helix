package shardmaster

import (
	"container/list"
	"encoding/gob"
	// "fmt"
	"labrpc"
	"raft"
	"sort"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	notify map[int][]chan Op
	Recv   chan raft.ApplyMsg
	Msgs   *list.List
	msgMu  sync.Mutex

	lastIncludeIndex int
	executeSeq       map[int64]int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Client   int64
	Sequence int

	Servers map[int][]string //for join
	GIDS    []int            //for leave
	//for move
	Shard int
	GID   int
	//query
	Num    int
	Config Config

	Type string
	Err  Err
}

type OpReply struct {
	Client      int64
	Sequence    int64
	Config      Config
	Type        string
	WrongLeader bool
	Err         Err
}

func (sm *ShardMaster) contains(arr []int, ele int) bool {
	for _, v := range arr {
		if v == ele {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) hasKey(data map[int]int, key int) bool {
	if _, ok := data[key]; ok {
		return true
	}
	return false
}

func (sm *ShardMaster) rebalance(shards []int, groups []int) {
	countShards := make(map[int]int, 0)
	for _, gid := range shards {
		if sm.contains(groups, gid) {
			if _, ok := countShards[gid]; !ok {
				countShards[gid] = 1
			} else {
				countShards[gid] += 1
			}
		}
	}
	sort.Ints(groups)
	mean := len(shards) / len(groups)

	for i := 0; i < 2; i++ {
		for shard, gid := range shards {
			if !sm.hasKey(countShards, gid) || countShards[gid] > mean+i {
				for _, newGid := range groups {
					if countShards[newGid] < mean+i {
						shards[shard] = newGid
						countShards[newGid]++
						if sm.hasKey(countShards, gid) {
							countShards[gid]--
						}
						break
					}
				}
			}
		}
	}
}

func (sm *ShardMaster) firstJoinBalance(shards []int, newServers []int) {
	mean := len(shards) / len(newServers)
	serverIndex := 0
	flag := 1
	for shard, _ := range shards {
		if flag > mean {
			serverIndex = (serverIndex + 1) % len(newServers)
			flag = 1
		}
		shards[shard] = newServers[serverIndex]
		flag++
	}
}

func (sm *ShardMaster) joinRebalance(shards []int, newServer []int) {
	gidShards := make(map[int][]int, 0)
	gids := make([]int, 0)
	for shard, gid := range shards {
		var data []int
		if _, ok := gidShards[gid]; !ok {
			data = make([]int, 0)
			gids = append(gids, gid)
		} else {
			data = gidShards[gid]
		}
		data = append(data, shard)
		gidShards[gid] = data
	}
	mean := len(shards) / (len(newServer) + len(gids))
	mod := len(shards) % (len(newServer) + len(gids))

	flag := 0
	meanF := 1
	transfer := 0
	for _, gid := range gids {
		gShards := gidShards[gid]
		if mod > 0 {
			mod--
			if len(gShards) > mean+1 {
				for len(gShards) > mean+1 {
					if meanF > mean {
						flag = (flag + 1) % len(newServer)
						meanF = 1
					}
					s := gShards[len(gShards)-1]
					if _, ok := gidShards[newServer[flag]]; !ok {
						shards[s] = newServer[flag]
						gShards = gShards[:len(gShards)-1]
						meanF++
						// fmt.Println(meanF, flag, newServer[flag])
					}
					transfer++
				}
			}
		} else {
			if len(gShards) > mean {
				for len(gShards) > mean {
					if meanF > mean {
						flag = (flag + 1) % len(newServer)
						meanF = 1
					}
					s := gShards[len(gShards)-1]
					if _, ok := gidShards[newServer[flag]]; !ok {
						shards[s] = newServer[flag]
						gShards = gShards[:len(gShards)-1]
						meanF++
					}
					transfer++
				}
			}
		}
	}
}
func (sm *ShardMaster) startRequest(op Op, reply *OpReply) {
	sm.mu.Lock()
	// fmt.Println("Join:", newConfig, sm.rf.GetRole())
	if _, ok := sm.executeSeq[op.Client]; !ok {
		sm.executeSeq[op.Client] = -1
	}

	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		// fmt.Println("Join: WrongLeader")
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	// fmt.Printf("%d receive request %s %d:%d\n", sm.me, op.Type, op.Client, op.Sequence)
	if _, ok := sm.notify[index]; !ok {
		sm.notify[index] = make([]chan Op, 0)
	}
	indexNotify := make(chan Op, 0)
	sm.notify[index] = append(sm.notify[index], indexNotify)
	sm.mu.Unlock()

	//waiting for notification
	var executeOp Op
	notified := false
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			break
		case <-time.After(10 * time.Millisecond):
			sm.mu.Lock()
			currentTerm, _ := sm.rf.GetState()
			if term != currentTerm {
				if sm.lastIncludeIndex < index {
					reply.WrongLeader = true
					delete(sm.notify, index)
					sm.mu.Unlock()
					return
				}
			}
			sm.mu.Unlock()
		}
		if notified {
			break
		}
	}
	reply.WrongLeader = false
	if executeOp.Client != op.Client || executeOp.Sequence != op.Sequence {
		reply.WrongLeader = true
		reply.Err = "FailCommit"
	} else {
		reply.Err = OK
		reply.Config = executeOp.Config
		reply.Type = executeOp.Type
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	//copy一份，可能args的server会指向别的
	servers := make(map[int][]string, 0)
	for gid := range args.Servers {
		servers[gid] = args.Servers[gid]
	}
	//start request
	op := Op{Client: args.Client, Sequence: args.Sequence, Servers: servers, Type: "Join"}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (sm *ShardMaster) applyJoin(op Op) {
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Num = lastConfig.Num + 1
	newConfig.Groups = make(map[int][]string, 0)
	newConfig.Shards = lastConfig.Shards
	gids := make([]int, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
		// gids = append(gids, k)
	}
	for k, v := range op.Servers {
		if _, ok := newConfig.Groups[k]; !ok {
			gids = append(gids, k)
		}
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//重新均衡的分配shards
	if len(sm.configs) == 1 {
		// fmt.Println("first join")
		sm.firstJoinBalance(newConfig.Shards[:], gids)
		// fmt.Println(newConfig.Shards)
	} else {
		// fmt.Println("apply Join", sm.configs, newConfig.Shards, gids)
		sm.joinRebalance(newConfig.Shards[:], gids)
		// fmt.Println(newConfig.Shards)
	}
	// sm.rebalance(newConfig.Shards[:], gids)
	// shardsNum := len(newConfig.Shards)
	// for i := 0; i < shardsNum; {
	// 	for j := 0; j < len(gids) && i < shardsNum; j++ {
	// 		newConfig.Shards[i] = gids[j]
	// 		i++
	// 	}
	// }
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	//start request
	op := Op{Client: args.Client, Sequence: args.Sequence, GIDS: args.GIDs, Type: "Leave"}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (sm *ShardMaster) applyLeave(op Op) {
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Groups = make(map[int][]string, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//delete gids
	for _, gid := range op.GIDS {
		delete(newConfig.Groups, gid)
	}
	gids := make([]int, 0)
	for k, _ := range newConfig.Groups {
		gids = append(gids, k)
	}
	//重新均衡的分配shards
	// shardsNum := len(newConfig.Shards)
	// for i := 0; i < shardsNum; {
	// 	for j := 0; j < len(gids) && i < shardsNum; j++ {
	// 		newConfig.Shards[i] = gids[j]
	// 		i++
	// 	}
	// }
	sm.rebalance(newConfig.Shards[:], gids)
	newConfig.Num = lastConfig.Num + 1
	// fmt.Println(sm.me, "Leave:", newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	//start request using raft
	op := Op{Client: args.Client, Sequence: args.Sequence, Shard: args.Shard, GID: args.GID, Type: "Move"}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (sm *ShardMaster) applyMove(op Op) {
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Groups = make(map[int][]string, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//新的shards
	for k, v := range lastConfig.Shards {
		newConfig.Shards[k] = v
	}
	newConfig.Num = lastConfig.Num + 1
	//Move
	newConfig.Shards[op.Shard] = op.GID
	sm.configs = append(sm.configs, newConfig)
}
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Client: args.Client, Sequence: args.Sequence, Num: args.Num, Type: "Query"}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	reply.Config = opReply.Config
	return
}

func (sm *ShardMaster) applyQuery(op *Op) {
	if op.Num == -1 || op.Num > len(sm.configs)-1 {
		op.Config = sm.configs[len(sm.configs)-1]
	} else {
		op.Config = sm.configs[op.Num]
	}
}

// func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
// 	// Your code here.
// 	lastConfig := sm.configs[len(sm.configs)-1]

// 	//start request
// 	if _, isLeader := sm.rf.GetState(); !isLeader {
// 		reply.WrongLeader = true
// 		return
// 	}
// 	reply.WrongLeader = false
// 	reply.Err = OK

// 	if args.Num == -1 {
// 		reply.Config = lastConfig
// 		return
// 	}
// 	//traverse the configs to find the config with Num
// 	for _, v := range sm.configs {
// 		if v.Num == args.Num {
// 			reply.Config = v
// 			return
// 		}
// 	}
// 	reply.Config = lastConfig
// 	return
// }

//apply request
func (sm *ShardMaster) applyCommand(msg raft.ApplyMsg) {
	sm.lastIncludeIndex = msg.Index
	index := msg.Index

	op := msg.Command.(Op)
	if _, ok := sm.executeSeq[op.Client]; !ok {
		sm.executeSeq[op.Client] = -1
	}
	if op.Sequence > sm.executeSeq[op.Client] {
		switch op.Type {
		case "Join":
			sm.applyJoin(op)
		case "Leave":
			//do something
			sm.applyLeave(op)
		case "Move":
			//do something
			sm.applyMove(op)
		case "Query":
			//do something
			sm.applyQuery(&op)
		}
		sm.executeSeq[op.Client] = op.Sequence
	} else {
		if op.Type == "Query" {
			sm.applyQuery(&op)
		}
	}

	if _, ok := sm.notify[index]; !ok {
		// fmt.Println(sm.me, "No request needed to be notified!")
		return
	}
	for _, c := range sm.notify[index] {
		sm.mu.Unlock()
		c <- op
		sm.mu.Lock()
	}
	delete(sm.notify, index)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.notify = make(map[int][]chan Op, 0)
	sm.Msgs = list.New()
	sm.Recv = make(chan raft.ApplyMsg, 0)
	sm.executeSeq = make(map[int64]int, 0)

	go func() {
		for {
			var (
				recvChan chan raft.ApplyMsg
				recvVal  raft.ApplyMsg
			)
			if sm.Msgs.Len() > 0 {
				recvChan = sm.Recv
				recvVal = sm.Msgs.Front().Value.(raft.ApplyMsg)
			}
			select {
			case msg := <-sm.applyCh:
				sm.Msgs.PushBack(msg)
				// fmt.Println(sm.me, "Receive msg from raft group", msg)
			case recvChan <- recvVal:
				sm.Msgs.Remove(sm.Msgs.Front())
			}
		}
	}()

	go func() {
		for {
			select {
			case msg := <-sm.Recv:
				//apply
				// fmt.Println(sm.me, "need to apply command")
				sm.mu.Lock()
				sm.applyCommand(msg)
				sm.mu.Unlock()
			}
		}
	}()
	return sm
}
