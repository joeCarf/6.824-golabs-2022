package shardctrler

import (
	"github.com/sasha-s/go-deadlock"
	"sort"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/7 19:04
 * @Func:
 **/

//==============定义线程安全的kv database, 就是底层的state machine=================
type CfgDB struct {
	mu      deadlock.RWMutex //保证互斥访问的锁
	configs []Config         //底层的数据库
}

//
// NewCfgDB
//  @Description: CfgDB的构造函数
//  @return *CfgDB
//
func NewCfgDB() *CfgDB {
	cfgdb := CfgDB{
		mu:      deadlock.RWMutex{},
		configs: make([]Config, 1),
	}
	cfgdb.configs[0].Groups = map[int][]string{}
	return &cfgdb
}

const InvalidGid = 0

//==========================CfgDB支持的四种操作=========================//

//
// Join
//  @Description: 增加raft分组group, 并做负载均衡
//  @receiver cf
//  @param group
//  @return Err
//
func (cf *CfgDB) Join(group map[int][]string) Err {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	newCfg := cf.newConfigDeepCopy()
	// 将group深拷贝到最新的config中
	for gid, servers := range group {
		if _, ok := newCfg.Groups[gid]; !ok {
			// 只有map中不包含这个gid, 才需要创建, 如果已经存在就跳过
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newCfg.Groups[gid] = newServers
		}
	}
	// 需要做负载均衡, load balance
	loadBalanceJoin(newCfg)
	cf.configs = append(cf.configs, *newCfg)
	//DPrintf(dJoin, "Join: [configs=%v]", cf.configs)
	return OK
}

//
// Leave
//  @Description:
//  @receiver cf
//  @param gids
//  @return Err
//
func (cf *CfgDB) Leave(gids []int) Err {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	newCfg := cf.newConfigDeepCopy()
	delShards := make([]int, 0) //存储将要分释放的shards
	g2sMap := newCfg.getMapGidToShards()
	DPrintf(dLeave, "newCfg=%v, g2sMap=%v", newCfg, g2sMap)
	// 从group中删除gid对应的内容, 同时从m2sMap中也删掉对应的map->shards
	for _, gid := range gids {
		if _, ok := newCfg.Groups[gid]; ok {
			// gid存在才能leave
			delete(newCfg.Groups, gid)
		}
		if shards, ok := g2sMap[gid]; ok {
			delShards = append(delShards, shards...)
			delete(g2sMap, gid)
		}
	}
	// 需要做负载均衡, load balance
	loadBalanceLeave(newCfg, delShards, g2sMap)
	cf.configs = append(cf.configs, *newCfg)
	return OK
}

//
// Move
//  @Description: Move操作, 设置分片shard对应的集群为gid
//  @receiver cf
//  @param shard
//  @param gid
//  @return Err
//
func (cf *CfgDB) Move(shard int, gid int) Err {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	//NOTE: 注意这里必须是深拷贝, 深拷贝创建一个新的config, 否则会与旧config指向同一片空间
	newCfg := cf.newConfigDeepCopy()
	newCfg.Shards[shard] = gid
	cf.configs = append(cf.configs, *newCfg)
	return OK
}

//
// Query
//  @Description: 查找num的配置, 如果存在返回, 不存在则返回最新配置
//  @receiver cf
//  @param num
//  @return Config
//
func (cf *CfgDB) Query(num int) (Config, Err) {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	// 如果num存在, 则返回对应配置, 不存在则返回最新配置
	if num >= 0 && num < len(cf.configs) {
		return cf.configs[num], OK
	} else {
		return cf.lastConfig(), OK
	}
}

//===========================一些功能函数===========================//
//
// lastConfig
//  @Description: 返回cf.configs的最新一项config
//  @receiver cf
//  @return Config
//
func (cf *CfgDB) lastConfig() Config {
	return cf.configs[len(cf.configs)-1]
}

//
// newConfigDeepCopy
//  @Description: 返回一个lastConfig的深拷贝
//  @receiver cf
//  @return *Config
//
func (cf *CfgDB) newConfigDeepCopy() *Config {
	lastcfg := cf.lastConfig()
	// deep copy lastcfg, 主要需要深拷贝其中的group map
	newGroup := make(map[int][]string)
	for gid, servers := range lastcfg.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroup[gid] = newServers
	}
	return &Config{
		Num:    len(cf.configs),
		Shards: lastcfg.Shards,
		Groups: newGroup,
	}
}

//==============================load balance相关辅助函数==========================//
//
// loadBalanceJoin
//  @Description: 对Join操作后的Config进行负载均衡
//  @param newCfg
//
func loadBalanceJoin(newCfg *Config) {
	// 先判断是不是shards还没有初始化, shards最初只能是发生第一个Join的时候赋初值
	isFirstJoin := true
	for _, gid := range newCfg.Shards {
		if gid != InvalidGid {
			// 只要shards中包含一个初始化过的shard->gid, 就是初始化过的
			isFirstJoin = false
			break
		}
	}
	// 对于第一次的Join, 要赋值shards
	if isFirstJoin {
		// NOTE: 这里必须保证, 所有节点选出来的第一个join的值是相同的, 所以必须对map排序
		groups := sortMapByKeysString(newCfg.Groups)
		if gids := groups; len(gids) > 0 {
			// 选择newCfg.Group中, 排序后最小的gid作为初始化的值
			gid := gids[0]
			for i := range newCfg.Shards {
				newCfg.Shards[i] = gid
			}
		}
	}
	g2sMap := newCfg.getMapGidToShards()
	// 负载均衡的方式, 就是找到shards最多和最少的gid, 将前者的一个分给后者, 直到差值小于1且maxGid !=0 (因为maxGid=0说明里面是空的)
	for {
		maxGid, maxSize, minGid, minSize := getMaxMinShardGid(g2sMap)
		if maxGid != 0 && maxSize-minSize <= 1 {
			break
		}
		// 将maxGid的一个shard挪给minGid
		g2sMap[minGid] = append(g2sMap[minGid], g2sMap[maxGid][0])
		g2sMap[maxGid] = g2sMap[maxGid][1:]
	}
	// 遍历g2sMap, 深拷贝得到newShards
	var newShards [NShards]int
	for gid, shards := range g2sMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newCfg.Shards = newShards
}

//
// loadBalanceLeave
//  @Description: 对Leave操作后的Config进行负载均衡
//  @param newCfg
//
func loadBalanceLeave(newCfg *Config, delShards []int, g2sMap map[int][]int) {
	//// 只有对应的raft组存在, 才需要作负载均衡
	//DPrintf(dLeave, "newCfg=%v, delShards =%v, g2sMap=%v", newCfg, delShards, g2sMap)
	for _, shard := range delShards {
		// 对要释放的每一个shard, 都放到具有shard最小的gid里
		_, _, minGid, _ := getMaxMinShardGid(g2sMap)
		g2sMap[minGid] = append(g2sMap[minGid], shard)
	}
	// 遍历g2sMap, 深拷贝得到newShards
	var newShards [NShards]int
	for gid, shards := range g2sMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newCfg.Shards = newShards
	//DPrintf(dLeave, "newCfg=%v, delShards =%v, g2sMap=%v", newCfg, delShards, g2sMap)
}

//
// getMapGidToShards
//  @Description: 获得config的一个map, 从gid->shards
//  @receiver c
//  @return map[int][]int
//
func (c *Config) getMapGidToShards() map[int][]int {
	g2sMap := make(map[int][]int)
	// 如果groups为空, 直接返回空map
	if len(c.Groups) == 0 {
		return g2sMap
	}
	// 根据groups初始化map
	for gid := range c.Groups {
		if gid == InvalidGid { //0需要跳过, 因为创建的时候插入了一个gid=0的group, 这个没有实际的组与之对应
			continue
		}
		g2sMap[gid] = make([]int, 0)
	}
	// 根据shards计算gid->shards的map
	for shard, gid := range c.Shards {
		if gid == InvalidGid {
			continue
		}
		g2sMap[gid] = append(g2sMap[gid], shard)
	}
	return g2sMap
}

//
// sortMapByKeys
//  @Description: 根据map的key, 对map进行排序, 返回排好序的key
//  @param m
//  @return []int
//
func sortMapByKeysInt(m map[int][]int) []int {
	// go中map不能直接排序, 必须转为切片再排序
	// 创建一个用于存储键的切片
	var keys []int
	for k := range m {
		keys = append(keys, k)
	}
	// 对键进行排序
	sort.Ints(keys)
	return keys
}
func sortMapByKeysString(m map[int][]string) []int {
	// go中map不能直接排序, 必须转为切片再排序
	// 创建一个用于存储键的切片
	var keys []int
	for k := range m {
		keys = append(keys, k)
	}
	// 对键进行排序
	sort.Ints(keys)
	return keys
}

//
// getMaxMinShardGid
//  @Description: 遍历所有gid对应的shards, 找出其中shard最多和最小的gid
//  @param m2sMap
//  @return int
//  @return int
//  @return int
//  @return int
//
func getMaxMinShardGid(m2sMap map[int][]int) (int, int, int, int) {
	// 初始化最大shard和最小shard的gid和size
	maxGid, maxSize := InvalidGid, -1
	minGid, minSize := InvalidGid, NShards

	//NOTE: go中map的遍历是无序的, 遍历前必须先对key排序, 才能保证获得一致的结果
	//获得有序的key
	keys := sortMapByKeysInt(m2sMap)

	//遍历所有gid, 获得其中shards最大和最小的
	for _, gid := range keys {
		shard := m2sMap[gid]
		if len(shard) > maxSize {
			maxGid = gid
			maxSize = len(shard)
		}
		if len(shard) < minSize {
			minGid = gid
			minSize = len(shard)
		}
	}
	return maxGid, maxSize, minGid, minSize
}
