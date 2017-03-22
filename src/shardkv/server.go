package shardkv

// import "shardmaster"
import (
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client   int64
	Sequence int64

	Key   string
	Value string

	Type string //Get or PutAppend
	Err  Err

	//for detected new config
	Config      shardmaster.Config
	Data        map[string]string
	Shard       int
	ExecutedSeq map[int64]int64
}

type OpReply struct {
	Client   int64
	Sequence int64

	Value       string
	WrongLeader bool
	Err         Err
}

type SendShardArgs struct {
	Data        map[string]string
	Shard       int
	ExecutedSeq map[int64]int64
	Config      shardmaster.Config
	NewConfig   shardmaster.Config
}

type SendShardReply struct {
	WrongLeader bool
	Err         Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	ExecutedSeq map[int64]int64
	notify      map[int][]chan OpReply

	Recv  chan raft.ApplyMsg
	Msgs  *list.List
	msgMu sync.Mutex

	lastIncludedIndex int
	lastIncludedTerm  int

	sm      *shardmaster.Clerk
	servers []*labrpc.ClientEnd
	//persistent
	persister *raft.Persister

	//
	CurConfigNum int
	Configs      []shardmaster.Config     //a copy of configs in shardmaster
	Shards       [shardmaster.NShards]int //indicate which shards the group owns
}

func (kv *ShardKV) persist() (int, int, bool) {
	lastIncludedIndex := 0
	lastIncludedTerm := 0
	r := bytes.NewBuffer(kv.persister.ReadSnapshot())
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.mu.Lock()
	curIndex := kv.lastIncludedIndex
	curTerm := kv.lastIncludedTerm
	if lastIncludedIndex >= curIndex {
		kv.mu.Unlock()
		return curIndex, curTerm, false
	}

	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)

	e.Encode(kv.CurConfigNum)
	e.Encode(kv.Shards)
	e.Encode(kv.ExecutedSeq)
	e.Encode(kv.data)

	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
	kv.mu.Unlock()
	return curIndex, curTerm, true
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)

	kv.data = make(map[string]string)
	d.Decode(&kv.lastIncludedIndex)
	d.Decode(&kv.lastIncludedTerm)
	d.Decode(&kv.CurConfigNum)
	d.Decode(&kv.Shards)
	d.Decode(&kv.ExecutedSeq)
	d.Decode(&kv.data)
}

func (kv *ShardKV) startRequest(op Op, reply *OpReply) {
	// fmt.Printf("Server %d Receive Put request %s=%s\n", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	if _, ok := kv.ExecutedSeq[op.Client]; !ok {
		kv.ExecutedSeq[op.Client] = -1
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make([]chan OpReply, 0)
	}
	indexNotify := make(chan OpReply)
	kv.notify[index] = append(kv.notify[index], indexNotify)
	kv.mu.Unlock()
	// fmt.Printf("Server %d wait for notify: %s\n", kv.me, args.Key)
	var executeOp OpReply
	notified := false
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			break
		case <-time.After(10 * time.Millisecond):
			kv.mu.Lock()
			if currentTerm, _ := kv.rf.GetState(); term != currentTerm {
				if kv.lastIncludedIndex < index {
					reply.WrongLeader = true
					delete(kv.notify, index)
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
		}
		if notified {
			// fmt.Printf("Server %d receive notify key %s\n", kv.me, executeOp.Key)
			break
		}
	}

	if executeOp.Client != op.Client || executeOp.Sequence != op.Sequence {
		reply.Err = "FailCommit"
		reply.WrongLeader = true
	} else {
		reply.Client = executeOp.Client
		reply.Sequence = executeOp.Sequence
		reply.WrongLeader = false
		reply.Err = executeOp.Err
		reply.Value = executeOp.Value
		// fmt.Printf("Request: %s, Result:%v\n", op.Type, executeOp)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Println("Begin to Get")
	// fmt.Printf("Get from server %d , shards:%v, Num:%d\n", kv.me, kv.Shards, kv.CurConfigNum)
	op := Op{Client: args.Client, Sequence: args.Sequence, Key: args.Key, Type: "Get"}
	opReply := &OpReply{}
	kv.startRequest(op, opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	reply.Value = opReply.Value
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("Server %d Receive Put request %s=%s\n", kv.me, args.Key, args.Value)
	op := Op{Client: args.Client, Sequence: args.Sequence, Key: args.Key, Value: args.Value, Type: args.Op}
	opReply := &OpReply{}
	kv.startRequest(op, opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (kv *ShardKV) makeSendShardArgs(shard int, oldconfig shardmaster.Config, newConfig shardmaster.Config) SendShardArgs {
	args := SendShardArgs{}
	args.Shard = shard
	args.Data = make(map[string]string)
	args.ExecutedSeq = make(map[int64]int64)
	args.Config = oldconfig
	args.NewConfig = newConfig
	for key := range kv.data {
		// s := key2shard(key) //must assign the return val to a new var?!!!!
		if key2shard(key) == shard {
			args.Data[key] = kv.data[key]
		}
	}
	for client := range kv.ExecutedSeq {
		args.ExecutedSeq[client] = kv.ExecutedSeq[client]
	}
	return args
}

func (kv *ShardKV) sendShard(args SendShardArgs) {
	servers := args.NewConfig.Groups[args.NewConfig.Shards[args.Shard]]
	for {
		select {
		case <-time.After(2 * time.Millisecond):
			kv.mu.Lock()
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				reply := SendShardReply{}
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.InstallShard", args, &reply)
				kv.mu.Lock()
				if ok && reply.WrongLeader == false && reply.Err == OK {
					//**fmt.Println("Server:", kv.me, ".gid:", kv.gid, "Send shard:", args.Shard, " to gid: ", args.NewConfig.Shards[args.Shard], " success.")
					// kv.deleteShard(args.Data, args.Shard)
					kv.deleteShard(args.Data, args.Shard, args.Config.Num)
					kv.mu.Unlock()
					//kv.persist()
					return
				}
			}
			kv.mu.Unlock()
		}
		//      <- time.After(5*time.Millisecond)
	}
}

func (kv *ShardKV) InstallShard(args SendShardArgs, reply *SendShardReply) {
	kv.mu.Lock()

	num := kv.Shards[args.Shard]
	if num >= args.Config.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{Type: "InstallShard", Data: args.Data, Shard: args.Shard, ExecutedSeq: args.ExecutedSeq, Config: args.NewConfig}
	op.Sequence = nrand()
	opreply := &OpReply{}
	kv.mu.Unlock()
	kv.startRequest(op, opreply)
	kv.mu.Lock()
	reply.WrongLeader = opreply.WrongLeader
	reply.Err = opreply.Err
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) deleteShard(data map[string]string, shard int, num int) {
	if kv.Shards[shard] <= num {
		for i := range data {
			if _, ok := kv.data[i]; ok {
				delete(kv.data, i)
			}
		}
		kv.Shards[shard] = 0
		// fmt.Printf("Server %d Delete Shard:%d, Maxraftstate:%d\n", kv.me, shard, kv.maxraftstate)
		if kv.maxraftstate > 0 {
			//don`t call kv.persist(), because it will not persist when kv.lastIncludedIndex does not change
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			e.Encode(kv.lastIncludedIndex)
			e.Encode(kv.lastIncludedTerm)

			e.Encode(kv.CurConfigNum)
			e.Encode(kv.Shards)
			e.Encode(kv.ExecutedSeq)
			e.Encode(kv.data)

			data := w.Bytes()
			kv.persister.SaveSnapshot(data)
			// kv.persist()
		}
	}

}

func (kv *ShardKV) applyInstallShard(op Op, res *OpReply) {
	if op.Config.Num > kv.Configs[len(kv.Configs)-1].Num {
		// fmt.Printf("Server %d gid: %d, wait config:%d,Cur Config:%d, Shard:%d\n", kv.me, kv.gid, op.Config, kv.Configs[len(kv.Configs)-1].Num)
		res.Err = "Wait"
	} else {
		// fmt.Printf("Server %d apply installshard %v\n", kv.me, kv.Shards)
		res.Err = OK
		if op.Config.Num > kv.Shards[op.Shard] {
			kv.Shards[op.Shard] = op.Config.Num
			for i, v := range op.Data {
				kv.data[i] = v
			}
			for i, v := range op.ExecutedSeq {
				if seq, ok := kv.ExecutedSeq[i]; !ok || seq < v {
					kv.ExecutedSeq[i] = v
				}
			}

			for i := 0; i < len(kv.Configs); i++ {
				if kv.Configs[i].Num > op.Config.Num {
					if kv.Configs[i].Shards[op.Shard] == kv.gid {
						// fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "move installed shard # to config:", kv.Configs[i], "kv.Shards[op.Shard]:", kv.Shards[op.Shard], "shards:", op.Shard)
						kv.Shards[op.Shard] = kv.Configs[i].Num
					} else {
						//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "send installed shard # to config:", kv.Configs[i], "kv.Shards[op.Shard]:", kv.Shards[op.Shard], "shards:", op.Shard, " from config:", kv.Configs[i-1])
						args := kv.makeSendShardArgs(op.Shard, kv.Configs[i-1], kv.Configs[i])
						go kv.sendShard(args)
						break
					}
				}
			}
		}
	}
}

func (kv *ShardKV) applyConfig(op Op, res *OpReply) {
	// fmt.Printf("Server %d apply config %v\n", kv.me, op.Config)
	if kv.Configs[len(kv.Configs)-1].Num >= op.Config.Num {
		res.Err = OK
		return
	}
	//should I append the new config to kv.Configs?
	oldConf := kv.Configs[len(kv.Configs)-1]
	kv.Configs = append(kv.Configs, op.Config)
	kv.CurConfigNum = op.Config.Num
	if oldConf.Num == 0 {
		for i, gid := range op.Config.Shards {
			if kv.gid == gid {
				kv.Shards[i] = op.Config.Num
			} else {
				kv.Shards[i] = 0
			}
		}
	} else {
		for i, gid := range op.Config.Shards {
			//just need to update the shards still owned by itself
			if kv.gid == gid && kv.Shards[i] == oldConf.Num {
				kv.Shards[i] = op.Config.Num
			}
		}
		// fmt.Printf("Server %d shards:%v\n", kv.me, kv.Shards)
		// send shards which no longer owed by itself
		for key, gid := range op.Config.Shards {
			if kv.Shards[key] == oldConf.Num && gid != kv.gid {
				// fmt.Printf("Server %d Send shards\n", kv.me)
				args := kv.makeSendShardArgs(key, oldConf, op.Config)
				// go kv.sendShard(args)
				go kv.sendShard(args)
			}
		}
	}
	res.Err = OK
}

//****************************

func (kv *ShardKV) applyCommand(msg raft.ApplyMsg) {
	if msg.UseSnapshot {
		kv.readPersist(msg.Snapshot)
		kv.Configs = make([]shardmaster.Config, 1)
		kv.Configs[0].Groups = map[int][]string{}
		if kv.CurConfigNum != 0 {
			for i := 1; i <= kv.CurConfigNum; i++ {
				cf := kv.sm.Query(i)
				kv.Configs = append(kv.Configs, cf)
			}
		}
		return
	}

	kv.lastIncludedIndex = msg.Index
	kv.lastIncludedTerm = msg.Term

	// fmt.Printf("ApplyCommand:%+v,Command:%v\n", msg, msg.Command)
	op := msg.Command.(Op)
	clientId := op.Client
	index := msg.Index
	opSequence := op.Sequence
	res := &OpReply{Client: clientId, Sequence: opSequence}

	if _, ok := kv.ExecutedSeq[clientId]; !ok {
		kv.ExecutedSeq[clientId] = -1
	}
	if op.Type == "Config" {
		kv.applyConfig(op, res)
	} else if op.Type == "InstallShard" {
		kv.applyInstallShard(op, res)
	} else if kv.gid != kv.Configs[len(kv.Configs)-1].Shards[key2shard(op.Key)] {
		// fmt.Printf("WrongGroup: kv.gid=%d, config.shards.gid=%d\n ", kv.gid, kv.Configs[len(kv.Configs)-1].Shards[key2shard(op.Key)])
		// fmt.Printf("Wrong Group: config:%v,Type:%s,ConfigNum:%d\n", kv.Configs, op.Type, kv.CurConfigNum)
		res.Err = ErrWrongGroup
	} else if kv.Shards[key2shard(op.Key)] < kv.Configs[len(kv.Configs)-1].Num {
		res.Err = "WaitReceive"
		// fmt.Printf("Server %d WaitReceive gid:%d,shard:%d,config:%v, shardConfig:%d\n", kv.me, kv.gid, key2shard(op.Key), kv.Configs[len(kv.Configs)-1].Num, kv.Shards[key2shard(op.Key)])
	} else if opSequence > kv.ExecutedSeq[clientId] {
		switch op.Type {
		case "Get":
			v, ok := kv.data[op.Key]
			if !ok {
				res.Err = ErrNoKey
			} else {
				res.Value = v
				res.Err = OK
			}
			// fmt.Println("Apply Get", op.Key, v)
		case "Put":
			// fmt.Println("Apply Put", op.Key, op.Value)
			kv.data[op.Key] = op.Value
			res.Err = OK
		case "Append":
			if _, ok := kv.data[op.Key]; ok {
				kv.data[op.Key] += op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
			res.Err = OK
		}
		kv.ExecutedSeq[clientId] = opSequence
	} else {
		res.Err = OK
		if op.Type == "Get" {
			v, ok := kv.data[op.Key]
			if !ok {
				res.Err = ErrNoKey
			} else {
				res.Value = v
			}
		}
	}

	if _, ok := kv.notify[index]; !ok {
		return
	}
	for _, c := range kv.notify[index] {
		kv.mu.Unlock()
		// fmt.Printf("Server %d send notify key %s, Data:%v\n", kv.me, res.Key, kv.data)
		c <- *res
		kv.mu.Lock()
	}
	delete(kv.notify, index)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.data = make(map[string]string, 0)
	kv.ExecutedSeq = make(map[int64]int64)

	kv.Msgs = list.New()
	kv.Recv = make(chan raft.ApplyMsg)
	kv.notify = make(map[int][]chan OpReply, 0)

	kv.lastIncludedTerm = 0
	kv.lastIncludedIndex = 0

	kv.sm = shardmaster.MakeClerk(masters)
	kv.servers = servers
	kv.Configs = make([]shardmaster.Config, 1)
	kv.Configs[0].Groups = map[int][]string{}
	kv.CurConfigNum = 0

	// fmt.Printf("Restart Server %d \n", kv.me)
	kv.readPersist(persister.ReadSnapshot())

	if kv.rf.GetFirstIndex() < kv.lastIncludedIndex {
		kv.rf.CutLog(kv.lastIncludedIndex, kv.lastIncludedTerm)
	} else if kv.rf.GetFirstIndex() > kv.lastIncludedIndex {
		fmt.Println("KvServer Error: FirstIndex > LastIncludedIndex")
	}

	//restore the configs if recovering from failure
	if kv.CurConfigNum != 0 {
		for i := 1; i <= kv.CurConfigNum; i++ {
			cf := kv.sm.Query(i)
			kv.Configs = append(kv.Configs, cf)
		}
	}
	// fmt.Printf("Start Server %d Config Num:%d, gid:%d\n", kv.me, kv.CurConfigNum, kv.gid)
	//
	n := len(kv.Configs)
	for key, _ := range kv.Configs[n-1].Shards {
		if kv.Shards[key] != 0 && kv.Shards[key] != kv.Configs[n-1].Num {
			for i := 1; i < len(kv.Configs); i++ {
				if kv.Configs[i].Num > kv.Shards[key] {
					if kv.Configs[i].Shards[key] != kv.gid {
						args := kv.makeSendShardArgs(key, kv.Configs[i-1], kv.Configs[i])
						go kv.sendShard(args)
						break
					}
				}
			}
		}
	}

	//waiting for applymsg
	go func() {
		for {
			var (
				recvChan chan raft.ApplyMsg
				recvVal  raft.ApplyMsg
			)
			if kv.Msgs.Len() > 0 {
				recvChan = kv.Recv
				recvVal = kv.Msgs.Front().Value.(raft.ApplyMsg)
			}
			select {
			case msg := <-kv.applyCh:
				kv.Msgs.PushBack(msg)
			case recvChan <- recvVal:
				kv.Msgs.Remove(kv.Msgs.Front())
			}
		}
	}()

	go func() {
		for {
			select {
			case msg := <-kv.Recv:
				kv.mu.Lock()
				kv.applyCommand(msg)
				kv.mu.Unlock()
			}
		}
	}()

	//snapshot
	go func() {
		if maxraftstate <= 0 {
			return
		}
		for {
			select {
			case <-time.After(1 * time.Millisecond):
				if kv.rf.BeginSnapshot() {
					if persister.RaftStateSize() >= maxraftstate*3/5 {
						index, term, ok := kv.persist()
						if ok {
							kv.rf.CutLog(index, term)
						}
					}
					kv.rf.EndSnapshot()
				}
			}
		}
	}()
	//detect configurations changing
	go func() {
		for {
			select {
			case <-time.After(50 * time.Millisecond):
				//check if there is a new configuration
				kv.mu.Lock()
				config := kv.sm.Query(-1)
				oldConfig := kv.Configs[len(kv.Configs)-1]
				// kv.mu.Unlock()
				if config.Num > oldConfig.Num {
					// if kv.rf.GetRole() == 2 { //LEADER
					// fmt.Printf("Server %d receive NewConfig:%v\n", kv.me, config)
					if config.Num > oldConfig.Num+1 {
						config = kv.sm.Query(oldConfig.Num + 1)
					}
					op := Op{Config: config, Type: "Config"}
					op.Sequence = nrand()
					// op.Client = int64(kv.me)
					opReply := &OpReply{}
					kv.mu.Unlock()
					// fmt.Printf("Server %d begin to replicate Config %v\n", kv.me, config)
					kv.startRequest(op, opReply)
					kv.mu.Lock()
					// fmt.Printf("Replicate Config %s\n ", configReply.Err)
					if opReply.WrongLeader == false && opReply.Err == OK {
						//
						// fmt.Printf("Server %d request for replicating config successfully\n", kv.me)
					}
					// }
				}
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}
