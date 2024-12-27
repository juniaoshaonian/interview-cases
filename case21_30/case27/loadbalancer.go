package case27

import (
	"context"
	"github.com/pkg/errors"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"io"
	"sync"
)

// 定义权重的上下限
const (
	RequestType = "requestType" //
)

type rwServiceNode struct {
	mutex                *sync.RWMutex
	conn                 balancer.SubConn
	readWeight           int32
	curReadWeight        int32
	efficientReadWeight  int32
	writeWeight          int32
	curWriteWeight       int32
	efficientWriteWeight int32
}

type RWBalancer struct {
	nodes []*rwServiceNode
}

func (r *RWBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(r.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight int32
	var selectedNode *rwServiceNode
	ctx := info.Ctx
	iswrite := r.isWrite(ctx)
	for _, node := range r.nodes {
		node.mutex.Lock()
		if iswrite {
			totalWeight += node.efficientWriteWeight
			node.curWriteWeight += node.efficientWriteWeight
			if selectedNode == nil || selectedNode.curWriteWeight < node.curWriteWeight {
				selectedNode = node
			}
		} else {
			totalWeight += node.efficientReadWeight
			node.curReadWeight += node.efficientReadWeight
			if selectedNode == nil || selectedNode.curReadWeight < node.curReadWeight {
				selectedNode = node
			}
		}
		node.mutex.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mutex.Lock()
	if r.isWrite(ctx) {
		selectedNode.curWriteWeight -= totalWeight
	} else {
		selectedNode.curReadWeight -= totalWeight
	}
	selectedNode.mutex.Unlock()
	return balancer.PickResult{
		SubConn: selectedNode.conn,
		Done: func(info balancer.DoneInfo) {
			selectedNode.mutex.Lock()
			defer selectedNode.mutex.Unlock()
			isDecrementError := info.Err != nil && (errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF))
			if r.isWrite(ctx) {
				if isDecrementError && selectedNode.efficientWriteWeight > 0 {
					selectedNode.efficientWriteWeight--
				} else if info.Err == nil {
					selectedNode.efficientWriteWeight++
				}
			} else {
				if isDecrementError && selectedNode.efficientReadWeight > 0 {
					selectedNode.efficientReadWeight--
				} else if info.Err == nil {
					selectedNode.efficientReadWeight++
				}
			}
		},
	}, nil
}

func (r *RWBalancer) isWrite(ctx context.Context) bool {
	val := ctx.Value(RequestType)
	if val == nil {
		return false
	}
	vv, ok := val.(int)
	if !ok {
		return false
	}
	return vv == 1
}

type WeightBalancerBuilder struct {
}

func (w *WeightBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]*rwServiceNode, 0, len(info.ReadySCs))
	for sub, subInfo := range info.ReadySCs {
		readWeight := subInfo.Address.Attributes.Value("read_weight").(int32)
		writeWeight := subInfo.Address.Attributes.Value("write_weight").(int32)

		nodes = append(nodes, &rwServiceNode{
			mutex:                &sync.RWMutex{},
			conn:                 sub,
			readWeight:           readWeight,
			curReadWeight:        readWeight,
			efficientReadWeight:  readWeight,
			writeWeight:          writeWeight,
			curWriteWeight:       writeWeight,
			efficientWriteWeight: writeWeight,
		})
	}
	return &RWBalancer{
		nodes: nodes,
	}
}
