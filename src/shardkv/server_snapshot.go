package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/11 11:26
 * @Func:
 **/

//========================快照持久化相关========================//
//
// persistWithSnapshot
//  @Description: 将KVServer的结构序列化为字节流
//  @receiver kv
//  @return []byte
//
func (kv *ShardKV) makeSnapshot() []byte {
	// 持久化KVServer中的结构, 包括KVdb和status
	// NOTE: 不能持久化一个mutex对象, 因此需要把除了mu以外的对象单独持久化
	db, status := kv.persistShardKVdb()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.lastApplied) != nil ||
		e.Encode(db) != nil ||
		e.Encode(status) != nil ||
		e.Encode(kv.lastOperations) != nil ||
		e.Encode(kv.lastConfig) != nil ||
		e.Encode(kv.currentConfig) != nil {
		DPrintf(DError, "server-%d-%d encode fail", kv.gid, kv.me)
	}
	DPrintf(DSnapshot, "server-%d-%d write persist: [lastApplied=%v, db=%v, status=%v, lastOperations=%v, lastConfig=%v, currentConfig=%v]",
		kv.gid, kv.me, kv.lastApplied, db, status, kv.lastOperations, kv.lastConfig, kv.currentConfig)
	return w.Bytes()
}

//
// installSnapshot
//  @Description: 安装快照, 就是反序列化字节流
//  @receiver kv
//  @param snapshot
//
func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		kv.lastApplied = 0
		kv.db = NewShardKVdb()
		kv.lastOperations = make(map[int64]Operation)
		kv.lastConfig = shardctrler.DefaultConfig()
		kv.currentConfig = shardctrler.DefaultConfig()
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastApplied int
	var db map[int]KVdb
	var status map[int]ShardStatus
	var lastOperations map[int64]Operation
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&db) != nil ||
		d.Decode(&status) != nil ||
		d.Decode(&lastOperations) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		DPrintf(DError, "server-%d-%d decode fail", kv.gid, kv.me)
		panic("encode fail")
	}
	kv.lastApplied = lastApplied
	kv.db = NewShardKVbySnapshot(db, status)
	kv.lastOperations = lastOperations
	kv.lastConfig = lastConfig
	kv.currentConfig = currentConfig
	DPrintf(DSnapshot, "server-%d-%d read persist: [lastApplied=%v, db=%v, status=%v lastOperations=%v, lastConfig=%v, currentConfig=%v]",
		kv.gid, kv.me, lastOperations, db, lastOperations, lastConfig, currentConfig)
}

func (kv *ShardKV) persistShardKVdb() (map[int]KVdb, map[int]ShardStatus) {
	db := make(map[int]KVdb)
	status := make(map[int]ShardStatus)
	for shardId, shard := range kv.db {
		db[shardId] = shard.GetKVdb()
		status[shardId] = shard.GetStatus()
	}
	return db, status
}

func NewShardKVbySnapshot(db map[int]KVdb, status map[int]ShardStatus) ShardKVdb {
	shardkvdb := NewShardKVdb()
	for shardId := range db {
		shardkvdb[shardId].SetKVdb(db[shardId])
		shardkvdb[shardId].SetStatus(status[shardId])
	}
	return shardkvdb
}
