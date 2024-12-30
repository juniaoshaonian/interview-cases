package interceptor

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"interview-cases/case11_20/case17/monitor"
	"log/slog"
	"sync/atomic"
	"time"
)

type MemoryLimiter struct {
	// 状态 0-正常 1-限流状态
	state int32
	// 获取监控数据的抽象
	mon monitor.Monitor
	// 间隔多久获取监控数据
	interval time.Duration
}

func NewMemoryLimiter(mon monitor.Monitor, interval time.Duration) *MemoryLimiter {
	m := &MemoryLimiter{
		mon:      mon,
		interval: interval,
	}
	go m.monitor()
	return m
}

func (m *MemoryLimiter) monitor() {
	ticker := time.NewTicker(m.interval)
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			usage, err := m.mon.GetMemoryUsage(ctx)
			cancel()
			if err != nil {
				// 记录一下日志
				slog.Error("获取监控信息失败", slog.Any("err", err))
				continue
			}
			// 超内存了
			if usage >= 80 {
				atomic.StoreInt32(&m.state, 1)
			} else if usage <= 60 {
				atomic.StoreInt32(&m.state, 0)
			}
		}
	}

}

func (m *MemoryLimiter) BuildServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if atomic.LoadInt32(&m.state) == 1 {
			// 当前处于限流状态
			return nil, errors.New("触发了限流")
		}
		resp, err = handler(ctx, req)
		return
	}
}
