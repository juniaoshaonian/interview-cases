package monitor

import "context"

type Monitor interface {
	Qps(ctx context.Context) (int, error)
}

