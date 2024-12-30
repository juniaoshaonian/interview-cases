package limiter

import (
	"context"
	"time"
)

type Mock struct {
	startTime int64
}

func (m *Mock) Qps(ctx context.Context) (int, error) {
	now := time.Now().Unix()
	diff := now - m.startTime
	if diff <= 7 {
		return 1200, nil
	}
	if diff < 12 {
		return 300, nil
	}
	if diff < 19 {
		return 600, nil
	}
	if diff < 23 {
		return 1200, nil
	}
	return 800, nil
}
