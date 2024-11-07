package limiter

import (
	"context"
	"testing"
	"time"
)

func CtxWithVip() context.Context {
	return context.WithValue(context.Background(), VipCtxKey, 1)
}

func TestVip(t *testing.T) {
	limiter := NewVipLimiter(1000, &Mock{
		startTime: time.Now().Unix(),
	})
	// vip用户能正常访问
	limiter.Limit(CtxWithVip())

}
