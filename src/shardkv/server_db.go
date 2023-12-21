package shardkv

import (
	"6.824/shardctrler"
	"github.com/sasha-s/go-deadlock"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/11 11:14
 * @Func:
 **/

//=============对外提供分片的KV database=================//
//ShardKV的databse就是ShardKVdb, 就是一个Shard的切片, map维护
type ShardKVdb map[int]*Shard

func NewShardKVdb() ShardKVdb {
	db := make(ShardKVdb)
	for i := 0; i < shardctrler.NShards; i++ {
		db[i] = NewShard()
	}
	return db
}

//一个分片Shard由来KVdb和Shard的状态组成
type Shard struct {
	Kvdb   KVdb
	Status ShardStatus
	Mu     deadlock.RWMutex //保证互斥访问的锁
}

type ShardStatus int

const (
	Service ShardStatus = iota
	Pull
	Pulled
	GC
)

func NewShard() *Shard {
	return &Shard{
		Kvdb:   NewKVdb(),
		Status: Service,
		Mu:     deadlock.RWMutex{},
	}
}

func (s *Shard) SetKVdb(vdb KVdb) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	for key, value := range vdb.Db {
		s.Kvdb.Db[key] = value
	}
}

func (s *Shard) GetKVdb() KVdb {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Kvdb.copy()
}

func (s *Shard) SetStatus(status ShardStatus) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Status = status
}
func (s *Shard) GetStatus() ShardStatus {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Status
}

func (s *Shard) Get(key string) (string, Err) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	val, err := s.Kvdb.Get(key)
	return val, err
}

func (s *Shard) Put(key string, val string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Kvdb.Put(key, val)
	return
}
func (s *Shard) Append(key string, val string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Kvdb.Append(key, val)
	return
}

// 这里相当于直接定义的内存里的kv, 实际上在生产级别的 KV 服务中，数据不可能全存在内存中，系统往往采用的是 LSM 的架构，例如 RocksDB.
type KVdb struct {
	Db map[string]string //用一个map来模拟kv数据库
}

// NewKVdb
//
//	@Description: KVdb的构造函数
//	@return *KVdb
func NewKVdb() KVdb {
	return KVdb{
		Db: make(map[string]string),
	}
}

func (kvdb *KVdb) copy() KVdb {
	newKvdb := make(map[string]string)
	for k, v := range kvdb.Db {
		newKvdb[k] = v
	}
	return KVdb{Db: newKvdb}
}

// Get
//
//	@Description: 从map中Get一个值, 如果key不存在, 返回null
//	@receiver k
//	@param key
//	@return string
//	@return Err
func (k *KVdb) Get(key string) (string, Err) {
	ret, ok := k.Db[key]
	if !ok {
		return "", ErrNoKey
	}
	return ret, OK
}

// Put
//
//	@Description: 替换key的值为val, 如果不存在则创建一个新的
//	@receiver k
//	@param key
//	@param val
func (k *KVdb) Put(key string, val string) {
	k.Db[key] = val
}

// Append
//
//	@Description: 增加args到key的值上, 如果不存在创建一个新的
//	@receiver k
//	@param key
//	@param val
func (k *KVdb) Append(key string, args string) {
	k.Db[key] += args
}
