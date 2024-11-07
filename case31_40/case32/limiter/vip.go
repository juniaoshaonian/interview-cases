package limiter

import (
	"context"
	"errors"
	"interview-cases/case31_40/case32/monitor"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	VipCtxKey       = "vip"
	HealthyState    = 0 // 健康状态 对所有用户都不做限流处理
	RateLimitState  = 1 // 限流状态当前状态只允许vip通过，其他用户都会拒绝
	RecoveringState = 2 // 恢复状态当前状态根据一个特定概率去拒绝普通用户
)

type VipLimiter struct {
	// qps上限
	qpsUpperLimit int
	mu            *sync.RWMutex
	// 普通用户的通过率，100，触发限流后就开始下降
	regularUserPassRate int
	state               uint32
	mon                 monitor.Monitor
}

func NewVipLimiter(qpsUpperLimit int, mon monitor.Monitor) *VipLimiter {
	limiter := &VipLimiter{
		qpsUpperLimit: qpsUpperLimit,
		mon:           mon,
	}
	go limiter.monitorLoop()
	return limiter
}

func (v *VipLimiter) isVip(ctx context.Context) bool {
	val := ctx.Value(VipCtxKey)
	isVip, ok := val.(int)
	if !ok {
		return false
	}
	return isVip == 1
}

func (v *VipLimiter) Limit(ctx context.Context) (bool, error) {
	// vip用户不会被限流
	if v.isVip(ctx) {
		return false, nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	switch v.state {
	case HealthyState:
		// 健康状态就直接通过
		return false, nil
	case RateLimitState:
		// 当前状态只允许vip通过
		return true, nil
	case RecoveringState:
		return rand.IntN(100) >= v.regularUserPassRate, nil
	default:
		return false, errors.New("未知状态")
	}

}

// 流程描述
// 三个状态 健康状态（处理所有请求） 限流状态（只处理vip请求） 限流恢复状态（按比例处理非vip用户的请求）
// 健康状态
// - 每一秒去获取qps，如果qps到达阈值（1000/s） 持续 N 秒。就进入限流状态
// 限流状态
// - 流量下降为 80% 以下，持续 N 秒，就开始限流恢复
// 限流恢复状态
// - 平滑恢复，先处理10%的非vip用户的请求，观察如果流量下降为85%以下持续N秒就继续放开普通用户的流量，如果超过阈值就减少普通用户的流量

func (v *VipLimiter) monitorLoop() {
	var count int
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		qps, err := v.mon.Qps(ctx)
		cancel()
		if err != nil {
			slog.Error("获取系统qps失败", slog.Any("error", err))
		}
		count = v.rateLimit(qps, count)
		time.Sleep(1 * time.Second)
	}
}

func (v *VipLimiter) rateLimit(qps int, count int) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch v.state {
	case HealthyState:
		return v.handleHealth(qps, count)
	case RateLimitState:
		return v.handleRateLimit(qps, count)
	case RecoveringState:
		return v.handleRecovering(qps, count)
	}
	return 0
}

// 处理健康状态
func (v *VipLimiter) handleHealth(qps int, count int) int {
	if qps >= v.qpsUpperLimit {
		count++
		if count >= 5 { //持续 5 秒高于阈值则进入限流状态
			v.state = RateLimitState
			v.regularUserPassRate = 100
			count = 0
			slog.Info("进入限流状态")
		}
	} else {
		count = 0
	}
	return count
}

// 处理限流状态
func (v *VipLimiter) handleRateLimit(qps int, count int) int {
	// 限流状态: 检查是否可以进入恢复状态
	if qps < int(float64(v.qpsUpperLimit)*0.8) {
		count++
		if count >= 5 { // 持续 5 秒低于 80% 的流量，则进入限流恢复状态
			v.state = RecoveringState
			v.regularUserPassRate = 10
			count = 0
			slog.Info("进入限流恢复状态，普通用户通过率设为 10%")
		}
	} else {
		count = 0
	}
	return count
}

// 处理恢复状态
func (v *VipLimiter) handleRecovering(qps int, count int) int {
	if qps < int(float64(v.qpsUpperLimit)*0.85) {
		count++
		if count >= 5 {
			v.regularUserPassRate += 10 // 每次增加10%的普通用户流量
			slog.Info("进入限流恢复状态，普通用户通过率设为 10%")
			if v.regularUserPassRate >= 100 {
				v.state = HealthyState
				v.regularUserPassRate = 100
				slog.Info("恢复健康")
			}
			count = 0
		}
	} else if qps >= v.qpsUpperLimit {
		// 超过阈值则减少普通用户流量
		v.regularUserPassRate -= 10
		if v.regularUserPassRate < 0 {
			v.regularUserPassRate = 0
		}
		count = 0
	}
	return count
}

