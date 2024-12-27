package case27

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
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
				conn:           &mockSubConn{name: "weight-4"},
				mu:             &sync.RWMutex{},
				readWeight:     1,
				curReadWeight:  1,
				writeWeight:    4,
				curWriteWeight: 4,
			},
			{
				conn:           &mockSubConn{name: "weight-3"},
				mu:             &sync.RWMutex{},
				readWeight:     2,
				curReadWeight:  2,
				writeWeight:    3,
				curWriteWeight: 3,
			},
			{
				conn:           &mockSubConn{name: "weight-2"},
				readWeight:     3,
				mu:             &sync.RWMutex{},
				curReadWeight:  3,
				writeWeight:    2,
				curWriteWeight: 2,
			},
			{
				conn:       &mockSubConn{name: "weight-1"},
				readWeight: 4,
				mu:         &sync.RWMutex{},

				curReadWeight:  4,
				writeWeight:    1,
				curWriteWeight: 1,
			},
		},
	}

	// 交叉进行读写操作，模拟加权轮询
	operations := []struct {
		requestType  int
		expectedName string
	}{
		{0, "weight-1"}, // Read
		{1, "weight-4"}, // Write
		{0, "weight-2"}, // Read
		{1, "weight-3"}, // Write
		{0, "weight-3"}, // Read
		{1, "weight-2"}, // Write
		{0, "weight-1"}, // Read
		{1, "weight-4"}, // Write
		{0, "weight-2"}, // Read
		{1, "weight-3"}, // Write
	}

	for i, op := range operations {
		ctx := context.WithValue(context.Background(), RequestType, op.requestType)
		pickRes, err := b.Pick(balancer.PickInfo{Ctx: ctx})
		require.NoError(t, err)
		assert.Equal(t, op.expectedName, pickRes.SubConn.(*mockSubConn).name, "Operation %d", i)
	}
}
