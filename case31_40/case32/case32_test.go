package case32

import (
	"interview-cases/case31_40/case32/limiter"
	"interview-cases/case31_40/case32/monitor"
	"testing"
)

func TestCase32(t *testing.T) {
	mon := monitor.NewRateLimitMon()
	// 初始化限流中间件
	limit := limiter.NewVipLimiter(1000, mon)
	limitMiddleware := NewRateLimitBuilder(mon, limit)
	StartServer(":8080", limitMiddleware.Build())
}
