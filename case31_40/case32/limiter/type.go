package limiter

import "context"

// Limiter 限流器抽象
type Limiter interface {
	// Limit 返回是否需要触发限流
	Limit(ctx context.Context) (bool, error)
}
