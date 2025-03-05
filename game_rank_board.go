package rankboard

import (
	"context"
	"fmt"
	"github.com/allegro/bigcache"
	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants/v2"
	"hash/crc32"
	"math"
	"sort"
	"sync"
	"time"
)

// LeaderboardService 定义排行榜服务接口
type LeaderboardService interface {
	UpdateScore(playerId string, score int, timestamp int)
	GetPlayerRank(playerId string) int
	GetTopN(n int) []RankInfo
	GetPlayerRankRange(playerId string, rangeSize int) []RankInfo
	GetDenseRank(playerId string) int
}

// RankInfo 表示玩家的排名信息
type RankInfo struct {
	PlayerID string
	Score    int
	Rank     int
}

// RedisRankBoard 是排行榜服务的实现
type RedisRankBoard struct {
	clusterClient *redis.ClusterClient // Redis Cluster 客户端
	cache         *bigcache.BigCache   // 本地缓存
	pool          *ants.Pool           // 协程池
	shardCount    int                  // 分片数
}

// 全局变量
var (
	once      sync.Once
	rankBoard *RedisRankBoard
	initErr   error
)

// NewRedisRankBoard 初始化 Redis Cluster 客户端、本地缓存和协程池
func NewRedisRankBoard(shardCount, poolSize, cacheExpire int, addrs []string) (*RedisRankBoard, error) {
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs, // Redis Cluster 节点地址
	})
	duration := time.Duration(cacheExpire) * time.Minute
	cache, err := bigcache.NewBigCache(bigcache.DefaultConfig(duration))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize bigcache: %v", err)
	}
	pool, err := ants.NewPool(poolSize, ants.WithOptions(ants.Options{
		ExpiryDuration: 10 * time.Second,
		Nonblocking:    true,
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ants pool: %v", err)
	}
	return &RedisRankBoard{
		clusterClient: clusterClient,
		cache:         cache,
		pool:          pool,
		shardCount:    shardCount,
	}, nil
}

// InitRankBoard 初始化并返回全局 RedisRankBoard 对象
func InitRankBoard(shardCount, poolSize, cacheExpire int, addrs []string) (*RedisRankBoard, error) {
	once.Do(func() {
		rankBoard, initErr = NewRedisRankBoard(shardCount, poolSize, cacheExpire, addrs)
	})
	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize RedisRankBoard: %v", initErr)
	}
	return rankBoard, nil
}

// getShardID 根据 playerId 计算分片 ID
func (r *RedisRankBoard) getShardID(playerId string) int {
	return int(crc32.ChecksumIEEE([]byte(playerId))) % r.shardCount
}

// getShardKey 生成分片的 ZSet 键名
func (r *RedisRankBoard) getShardKey(shardID int) string {
	return fmt.Sprintf("playerRank:shard:%d", shardID)
}

// UpdateScore 更新玩家的分数和时间戳
func (r *RedisRankBoard) UpdateScore(ctx context.Context, playerId string, score int, timestamp int) {
	shardID := r.getShardID(playerId)
	key := r.getShardKey(shardID)
	// 组合分数：score + (1 - timestamp/1e12)
	combinedScore := float64(score) + (1 - float64(timestamp)/1e12)

	r.clusterClient.ZAdd(ctx, key, &redis.Z{
		Score:  combinedScore,
		Member: playerId,
	})
}

// GetPlayerRank 获取玩家的全局排名
func (r *RedisRankBoard) GetPlayerRank(ctx context.Context, playerId string) int {
	// 检查本地缓存
	if cached, err := r.cache.Get(playerId); err == nil {
		return int(cached[0]) // 假设缓存中存储的是字节形式的排名
	}

	// 使用协程池并发获取所有分片的局部排名
	var wg sync.WaitGroup
	localRankChan := make(chan int, r.shardCount)
	for i := 0; i < r.shardCount; i++ {
		wg.Add(1)
		shard := i
		_ = r.pool.Submit(func() {
			defer wg.Done()
			key := r.getShardKey(shard)
			// 使用 ZRevRank 获取局部排名
			localRank, err := r.clusterClient.ZRevRank(ctx, key, playerId).Result()
			if err != nil {
				if err == redis.Nil {
					localRankChan <- 0 // 玩家不在此分片，贡献 0 个更高排名
				} else {
					localRankChan <- 0 // 错误处理
				}
				return
			}
			localRankChan <- int(localRank)
		})
	}

	// 异步等待所有任务完成并关闭通道
	go func() {
		wg.Wait()
		close(localRankChan)
	}()

	// 计算全局排名：所有分片中排名更高的玩家总数
	totalHigher := 0
	for localRank := range localRankChan {
		totalHigher += localRank
	}
	globalRank := totalHigher + 1

	// 缓存全局排名
	r.cache.Set(playerId, []byte{byte(globalRank)})

	return globalRank
}

// GetTopN 获取前 N 名玩家
func (r *RedisRankBoard) GetTopN(ctx context.Context, n int) []RankInfo {
	var wg sync.WaitGroup
	resultChan := make(chan []redis.Z, r.shardCount)
	for i := 0; i < r.shardCount; i++ {
		wg.Add(1)
		shard := i
		_ = r.pool.Submit(func() {
			defer wg.Done()
			key := r.getShardKey(shard)
			res, _ := r.clusterClient.ZRevRangeWithScores(ctx, key, 0, int64(n)-1).Result()
			resultChan <- res
		})
	}

	wg.Wait()
	close(resultChan)

	var allScores []redis.Z
	for res := range resultChan {
		allScores = append(allScores, res...)
	}

	sort.Slice(allScores, func(i, j int) bool {
		return allScores[i].Score > allScores[j].Score
	})

	if len(allScores) > n {
		allScores = allScores[:n]
	}

	return parseResult(allScores)
}

// GetPlayerRankRange 获取玩家排名附近的范围
func (r *RedisRankBoard) GetPlayerRankRange(ctx context.Context, playerId string, rangeSize int) []RankInfo {
	rank := r.GetPlayerRank(ctx, playerId)
	if rank == -1 {
		return nil
	}

	start := max(0, rank-1-rangeSize)
	end := start + 2*rangeSize - 1

	var wg sync.WaitGroup
	resultChan := make(chan []redis.Z, r.shardCount)
	for i := 0; i < r.shardCount; i++ {
		wg.Add(1)
		shard := i
		_ = r.pool.Submit(func() {
			defer wg.Done()
			key := r.getShardKey(shard)
			res, _ := r.clusterClient.ZRevRangeWithScores(ctx, key, int64(start), int64(end)).Result()
			resultChan <- res
		})
	}

	wg.Wait()
	close(resultChan)

	var allScores []redis.Z
	for res := range resultChan {
		allScores = append(allScores, res...)
	}

	sort.Slice(allScores, func(i, j int) bool {
		return allScores[i].Score > allScores[j].Score
	})

	return parseResult(allScores)
}

// GetDenseRank 获取玩家的密集排名
func (r *RedisRankBoard) GetDenseRank(ctx context.Context, playerId string) int {
	shardID := r.getShardID(playerId)
	key := r.getShardKey(shardID)

	combinedScore, err := r.clusterClient.ZScore(ctx, key, playerId).Result()
	if err != nil {
		return -1
	}

	var wg sync.WaitGroup
	uniqueHigherChan := make(chan int, r.shardCount)
	for i := 0; i < r.shardCount; i++ {
		wg.Add(1)
		shard := i
		_ = r.pool.Submit(func() {
			defer wg.Done()
			key := r.getShardKey(shard)
			members, _ := r.clusterClient.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
				Min: fmt.Sprintf("(%f", combinedScore),
				Max: "+inf",
			}).Result()

			uniqueScores := make(map[float64]struct{})
			for _, m := range members {
				uniqueScores[m.Score] = struct{}{}
			}
			uniqueHigherChan <- len(uniqueScores)
		})
	}

	go func() {
		wg.Wait()
		close(uniqueHigherChan)
	}()

	totalUniqueHigher := 0
	for count := range uniqueHigherChan {
		totalUniqueHigher += count
	}

	return totalUniqueHigher + 1
}

// parseResult 将 Redis ZSet 结果转换为 RankInfo
func parseResult(zs []redis.Z) []RankInfo {
	result := make([]RankInfo, len(zs))
	for i, z := range zs {
		score := int(math.Floor(z.Score))
		result[i] = RankInfo{
			PlayerID: z.Member.(string),
			Score:    score,
			Rank:     i + 1,
		}
	}
	return result
}
