package shardctrler

import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs       []Config                 // indexed by config num
	clientRequest map[int64]int            // clientId -> the maximum request ID
	notifyCh      map[int64]chan NotifyMsg // 用于Apply后通知响应的Handler
}

type NotifyMsg struct {
	Err    Err
	Config Config // 若是Query命令,则使用该参数返回查询到的配置
}

type Op struct {
	// Your data here.
	CtrlType  CtrlType         // Ctrl命令的类型
	Servers   map[int][]string // Join命令的参数,new GID -> servers mappings
	GIDs      []int            // Leave命令的参数,需要删除的Group的ID
	Shard     int              // Move命令的参数,需要移动的Shard的ID
	GID       int              // Move命令的参数,需要移动到的Group的ID
	Num       int              // Query命令的参数,查询的配置ID
	SeqID     int64            // 该命令的唯一ID,用于Apply后的响应
	ClientId  int64            // 客户端id
	RequestId int              // 请求id
}

func distributeShards(groups map[int][]string) (shards [NShards]int) {
	gIds := make([]int, len(groups))
	i := 0
	for gId := range groups {
		gIds[i] = gId
		i++
	}
	// 对groups的编号，保存到了gIds切片里然后做了个排序
	sort.Ints(gIds)
	gIdsLen := len(gIds)
	averageNum := NShards / gIdsLen
	if averageNum == 0 {
		for shard := range shards {
			shards[shard] = gIds[shard]
		}
		return
	}
	for shard := range shards {
		// 简单来说就是每个group都分配averageNum个shard，如果group分配完了还有多的shard没被分配，那就塞到最后一个groupx
		if shard/averageNum >= gIdsLen {
			shards[shard] = gIds[gIdsLen-1]
		} else {
			shards[shard] = gIds[shard/averageNum]
		}
	}
	return
}

func (sc *ShardCtrler) closeCh(seqId int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	close(sc.notifyCh[seqId])
	delete(sc.notifyCh, seqId)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("ShardCtrler[%d]: 接收到client[%d],requestId=[%d]的Join请求: %v\n", sc.me, args.ClientId, args.RequestId, args)
	reply.WrongLeader = false
	sc.mu.Lock()
	//判断是否是重复请求
	if id, ok := sc.clientRequest[args.ClientId]; ok && id >= args.RequestId {
		reply.Err = ErrRepeatRequest
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		CtrlType: Join,
		Servers:  args.Servers,
		SeqID:    nrand(),
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// 若为Leader,则创建chan为了apply之后的notify
	notifyChannel := make(chan NotifyMsg)
	sc.mu.Lock()
	sc.notifyCh[op.SeqID] = notifyChannel
	sc.mu.Unlock()

	select {
	case notifyMsg := <-notifyChannel:
		reply.Err = notifyMsg.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go sc.closeCh(op.SeqID)
}

func (sc *ShardCtrler) ApplyJoin(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 增加配置
	config := Config{}
	config.Num = sc.configs[len(sc.configs)-1].Num + 1
	if config.Num == 1 {
		// 当前新增的组为第一组,那么直接添加新建
		config.Groups = op.Servers
		// 完成shard -> gid
		config.Shards = distributeShards(config.Groups)
	} else {
		// 否则,需要从前一个配置中进行添加
		lastConfig := sc.configs[len(sc.configs)-1]
		config.Groups = make(map[int][]string)
		// 将上一份的Groups复制到当前的config中
		for gId, servers := range lastConfig.Groups {
			config.Groups[gId] = servers
		}
		// 添加新增的Groups
		for gId, servers := range op.Servers {
			config.Groups[gId] = servers
		}
		config.Shards = distributeShards(config.Groups)
	}
	sc.configs = append(sc.configs, config)
	// 更新clientRequest
	sc.clientRequest[op.ClientId] = op.RequestId
	DPrintf("ShardCtrler[%d]: 现在configs: %v\n", sc.me, sc.configs)
	// 通知handler
	if ch, ok := sc.notifyCh[op.SeqID]; ok {
		ch <- NotifyMsg{
			Err:    OK,
			Config: Config{},
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("ShardCtrler[%d]: 接收到client[%d],requestId=[%d]的Leave请求: %v\n", sc.me, args.ClientId, args.RequestId, args)
	reply.WrongLeader = false
	sc.mu.Lock()
	if id, ok := sc.clientRequest[args.ClientId]; ok && id >= args.RequestId {
		reply.Err = ErrRepeatRequest
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		CtrlType: Leave,
		GIDs:     args.GIDs,
		SeqID:    nrand(),
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// 若为Leader,则创建chan为了apply之后的notify
	notifyChannel := make(chan NotifyMsg)
	sc.mu.Lock()
	sc.notifyCh[op.SeqID] = notifyChannel
	sc.mu.Unlock()

	select {
	case notifyMsg := <-notifyChannel:
		reply.Err = notifyMsg.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go sc.closeCh(op.SeqID)
}

func (sc *ShardCtrler) ApplyLeave(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	config := Config{}
	lastConfig := sc.configs[len(sc.configs)-1]
	config.Num = len(sc.configs)
	config.Groups = make(map[int][]string)
	// 将上一份的Groups复制到当前的config中
	for gId, servers := range lastConfig.Groups {
		config.Groups[gId] = servers
	}
	// 删去需要Leave的Gid
	for _, gId := range op.GIDs {
		delete(config.Groups, gId)
	}
	// 获取新配置的gid列表
	if len(config.Groups) > 0 {
		config.Shards = distributeShards(config.Groups)
	}
	sc.configs = append(sc.configs, config)
	// 更新clientRequest
	sc.clientRequest[op.ClientId] = op.RequestId
	// 通知handler
	if ch, ok := sc.notifyCh[op.SeqID]; ok {
		ch <- NotifyMsg{Err: OK, Config: Config{}}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("ShardCtrler[%d]: 接收到client[%d],requestId=[%d]的Move请求: %v\n", sc.me, args.ClientId, args.RequestId, args)
	reply.WrongLeader = false
	sc.mu.Lock()
	if id, ok := sc.clientRequest[args.ClientId]; ok && id >= args.RequestId {
		reply.Err = ErrRepeatRequest
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		CtrlType: Move,
		Shard:    args.Shard,
		GID:      args.GID,
		SeqID:    nrand(),
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// 若为Leader,则创建chan为了apply之后的notify
	notifyChannel := make(chan NotifyMsg)
	sc.mu.Lock()
	sc.notifyCh[op.SeqID] = notifyChannel
	sc.mu.Unlock()

	select {
	case notifyMsg := <-notifyChannel:
		reply.Err = notifyMsg.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go sc.closeCh(op.SeqID)
}

func (sc *ShardCtrler) ApplyMove(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 创建一个和上一个一样的新配置
	lastConfig := sc.configs[len(sc.configs)-1]
	config := Config{lastConfig.Num + 1, lastConfig.Shards, lastConfig.Groups}
	// 更新请求的shard->gid
	config.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, config)
	// 更新clientRequest
	sc.clientRequest[op.ClientId] = op.RequestId
	// 通知handler
	if ch, ok := sc.notifyCh[op.SeqID]; ok {
		ch <- NotifyMsg{OK, Config{}}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("ShardCtrler[%d]: 接收到client[%d],requestId=[%d]的Query请求: %v\n", sc.me, args.ClientId, args.RequestId, args)
	reply.WrongLeader = false
	sc.mu.Lock()
	// 若是已知的config就直接返回(因为config不会删除或者修改)
	if args.Num > 0 && args.Num < len(sc.configs) {
		reply.Err = OK
		reply.Config = sc.configs[args.Num]
		if request, ok := sc.clientRequest[args.ClientId]; ok {
			Max := func(a, b int) int {
				if a > b {
					return a
				}
				return b
			}
			sc.clientRequest[args.ClientId] = Max(request, args.RequestId)
		} else {
			sc.clientRequest[args.ClientId] = request
		}
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		CtrlType: Query,
		Num:      args.Num,
		SeqID:    nrand(),
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 若为Leader,则创建chan为了apply之后的notify
	notifyChannel := make(chan NotifyMsg)
	sc.mu.Lock()
	sc.notifyCh[op.SeqID] = notifyChannel
	sc.mu.Unlock()
	select {
	case notifyMsg := <-notifyChannel:
		reply.Err = notifyMsg.Err
		reply.Config = notifyMsg.Config
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go sc.closeCh(op.SeqID)
}

func (sc *ShardCtrler) ApplyQuery(op Op) (config Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 若请求配置编号为-1或者超过已知最大的编号,那么回复最新的配置
	if op.Num == -1 || op.Num >= len(sc.configs) {
		config = sc.configs[len(sc.configs)-1]
	} else {
		config = sc.configs[op.Num]
	}
	//更新clientRequest
	sc.clientRequest[op.ClientId] = op.RequestId
	//通知handler
	if ch, ok := sc.notifyCh[op.SeqID]; ok {
		ch <- NotifyMsg{Err: OK, Config: config}
	}
	return config
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ReceiveApplyMsg() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			DPrintf("ShardCtrler[%d]: 获取到applyCh中新的applyMsg=[%v]\n", sc.me, applyMsg)
			if applyMsg.CommandValid {
				// 当为合法命令时
				sc.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				// 当为合法快照时
				sc.ApplySnapshot(applyMsg)
			} else {
				// 非法消息
				DPrintf("ShardCtrler[%d]: error applyMsg from applyCh: %v\n", sc.me, applyMsg)
			}
		}
	}
}

func (sc *ShardCtrler) ApplyCommand(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	switch op.CtrlType {
	case Join:
		sc.ApplyJoin(op)
	case Leave:
		sc.ApplyLeave(op)
	case Move:
		sc.ApplyMove(op)
	case Query:
		sc.ApplyQuery(op)
	}
}

func (sc *ShardCtrler) ApplySnapshot(msg raft.ApplyMsg) {
	// 这里lab不需要实现快照，所以直接返回
	return
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.clientRequest = make(map[int64]int)

	// Your code here.

	sc.notifyCh = make(map[int64]chan NotifyMsg)
	go sc.ReceiveApplyMsg()
	return sc
}
