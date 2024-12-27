package case27

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
	"io"
	"sync"
	"testing"
)

type mockSubConn struct {
	name string
}

func (m *mockSubConn) UpdateAddresses([]resolver.Address) {}
func (m *mockSubConn) Connect()                           {}

func TestRWBalancer_Pick(t *testing.T) {
	b := &RWBalancer{
		nodes: []*rwServiceNode{
			{
				conn:                 &mockSubConn{name: "weight-4"},
				mutex:                &sync.RWMutex{},
				readWeight:           1,
				curReadWeight:        1,
				efficientReadWeight:  1,
				writeWeight:          4,
				curWriteWeight:       4,
				efficientWriteWeight: 4,
			},
			{
				conn:                 &mockSubConn{name: "weight-3"},
				mutex:                &sync.RWMutex{},
				readWeight:           2,
				curReadWeight:        2,
				efficientReadWeight:  2,
				writeWeight:          3,
				curWriteWeight:       3,
				efficientWriteWeight: 3,
			},
			{
				conn:                 &mockSubConn{name: "weight-2"},
				mutex:                &sync.RWMutex{},
				readWeight:           3,
				curReadWeight:        3,
				efficientReadWeight:  3,
				writeWeight:          2,
				curWriteWeight:       2,
				efficientWriteWeight: 2,
			},
			{
				conn:                 &mockSubConn{name: "weight-1"},
				mutex:                &sync.RWMutex{},
				readWeight:           4,
				curReadWeight:        4,
				efficientReadWeight:  4,
				writeWeight:          1,
				curWriteWeight:       1,
				efficientWriteWeight: 1,
			},
		},
	}

	// 交叉进行读写操作，模拟加权轮询
	operations := []struct {
		requestType  int
		expectedName string
		err          error
	}{
		{0, "weight-1", nil},                      // Read
		{1, "weight-4", context.DeadlineExceeded}, // Write with error
		{0, "weight-2", nil},                      // Read
		{1, "weight-3", io.EOF},                   // Write with error
		{0, "weight-3", nil},                      // Read
		{1, "weight-2", nil},                      // Write
		{0, "weight-1", nil},                      // Read
		{1, "weight-4", nil},                      // Write
		{0, "weight-2", nil},                      // Read
		{1, "weight-3", nil},                      // Write
	}

	for i, op := range operations {
		ctx := context.WithValue(context.Background(), RequestType, op.requestType)
		pickRes, err := b.Pick(balancer.PickInfo{Ctx: ctx})
		require.NoError(t, err)
		assert.Equal(t, op.expectedName, pickRes.SubConn.(*mockSubConn).name, "Operation %d", i)
		pickRes.Done(balancer.DoneInfo{Err: op.err})
	}
}
