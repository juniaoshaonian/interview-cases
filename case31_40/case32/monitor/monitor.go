package monitor

import (
	"context"
	"sync/atomic"
)

type RateLimitMon struct {
	count int32
}

func NewRateLimitMon() *RateLimitMon{
	return &RateLimitMon{
	}
}

// Qps 获取当前活跃的请求数
func (r *RateLimitMon) Qps(ctx context.Context) (int, error) {
	return int(atomic.LoadInt32(&r.count)), nil
}


func (r *RateLimitMon)InCr(){
	atomic.AddInt32(&r.count,1)
}

func (r *RateLimitMon)Decr() {
	atomic.AddInt32(&r.count,-1)
}