package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	sync.Mutex
	// Your definitions here.
	kvPair  map[string]string
	history map[int64]string
}

func (kv *KVServer) lookupHistory(id int64) (string, bool) {
	if val, ok := kv.history[id]; ok {
		return val, true
	}
	return "", false
}

func (kv *KVServer) addHistory(id int64, value string) {
	kv.history[id] = value
}

// default support idempotent
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.Lock()
	defer kv.Unlock()

	val := kv.kvPair[args.Key]
	reply.Value = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Lock()
	defer kv.Unlock()

	if history, ok := kv.lookupHistory(args.Id); ok {
		reply.Value = history
		return
	}

	kv.addHistory(args.Id, args.Value)
	kv.kvPair[args.Key] = args.Value
	reply.Value = args.Value
}

// default support idempotent
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Lock()
	defer kv.Unlock()

	if history, ok := kv.lookupHistory(args.Id); ok {
		reply.Value = history
		return
	}

	oldVal := kv.kvPair[args.Key]
	kv.addHistory(args.Id, oldVal)
	kv.kvPair[args.Key] = oldVal + args.Value
	reply.Value = oldVal

}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.Lock()
	defer kv.Unlock()

	delete(kv.history, args.Id)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvPair = make(map[string]string)
	kv.history = make(map[int64]string)
	return kv
}
