package limiter

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)



func TestVip(t *testing.T) {
	limiter := NewVipLimiter(1000, &Mock{
		startTime: time.Now().Unix(),
	})
	// vip用户能正常访问

	time.Sleep(7 * time.Second)
	// 触发限流
	state, passRate := limiter.getStateAndPassRate()
	assert.Equal(t, RateLimitState, state)

	// 触发限流恢复
	time.Sleep(6 * time.Second)
	state, passRate = limiter.getStateAndPassRate()
	assert.Equal(t, RecoveringState, state)
	// 普通用户只有10%被放行
	assert.Equal(t, 10, passRate)

	// 继续扩大普通用户的流量
	time.Sleep(6 * time.Second)
	state, passRate = limiter.getStateAndPassRate()
	assert.Equal(t, RecoveringState, state)
	// 普通用户20%被放行
	assert.Equal(t, 20, passRate)

	// 超过阈值开始减少普通用户的流量
	time.Sleep(4 * time.Second)
	state, passRate = limiter.getStateAndPassRate()
	assert.Equal(t, RecoveringState, state)
	// 普通用户20%被放行
	assert.Equal(t, 10, passRate)

	// 恢复健康所有用户都可以处理
	time.Sleep(46 * time.Second)
	state, passRate = limiter.getStateAndPassRate()
	assert.Equal(t, HealthyState, state)
	assert.Equal(t, 100, passRate)

}
