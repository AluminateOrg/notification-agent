package queue

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	key string
}

func NewRedisQueue(addr string) *RedisQueue {
	c := redis.NewClient(&redis.Options{Addr: addr})
	return &RedisQueue{client: c, key: "na:delayed"}
}

func (rq *RedisQueue) PushDelayed(ctx context.Context, id string, at time.Time) error {
	score := float64(at.Unix())
	return rq.client.ZAdd(ctx, rq.key, redis.Z{Score: score, Member: id}).Err()
}

//pops items whose score <= now (returns up to n items)
func (rq *RedisQueue) PopDue(ctx context.Context, n int) ([]string, error) {
	now := time.Now().Unix()
	ids, err := rq.client.ZRangeByScore(ctx, rq.key, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now, 10), Offset: 0, Count: int64(n)}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return ids, nil
	}
	//remove poped
	if err := rq.client.ZRem(ctx, rq.key, ids).Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func interfaceSlice(s []string) []interface{} {
	r := make([]interface{}, len(s))
	for i := range s {r[i] = s[i]}
	return r
}

//returns up to n members that are <= now and removes them
func (r *RedisQueue) PopReady(ctx context.Context, n int) ([]string, error) {
	now := time.Now().Unix()
	res, err := r.client.ZRangeByScore(ctx, r.key, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now, 10), Offset: 0, Count: int64(n),
	}).Result()
	if err != nil || len(res) == 0 {
		return res, err
	}
	//remove popped
	if err := r.client.ZRem(ctx, r.key, interfaceSlice(res)...).Err(); err != nil {
		return nil, err
	}
	return res, nil
}



func (rq *RedisQueue) Allow(ctx context.Context, org string, limit int, window time.Duration) (bool, error) {
	key := "rate:" + org
	val, err := rq.client.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if val == 1 {
		_ = rq.client.Expire(ctx, key, window).Err()
	}
	return val <= int64(limit), nil
}