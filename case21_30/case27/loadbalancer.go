package case27

import (
	"context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"sync"
)

// 定义权重的上下限
const (
	RequestType = "requestType" //
)

type rwServiceNode struct {
	mu             *sync.RWMutex
	readWeight     int32
	curReadWeight  int32
	writeWeight    int32
	curWriteWeight int32
	conn           balancer.SubConn
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

	for _, node := range r.nodes {
		node.mu.Lock()
		if r.isWrite(ctx) {
			// 写请求，使用写权重
			totalWeight += node.writeWeight
			node.curWriteWeight += node.writeWeight
			if selectedNode == nil || selectedNode.curWriteWeight < node.curWriteWeight {
				selectedNode = node
			}
		} else {
			// 读请求，使用读权重
			totalWeight += node.readWeight
			node.curReadWeight += node.readWeight
			if selectedNode == nil || selectedNode.curReadWeight < node.curReadWeight {
				selectedNode = node
			}
		}
		node.mu.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mu.Lock()
	if r.isWrite(ctx) {
		selectedNode.curWriteWeight -= totalWeight
	} else {
		selectedNode.curReadWeight -= totalWeight
	}
	selectedNode.mu.Unlock()

	return balancer.PickResult{
		SubConn: selectedNode.conn,
		Done: func(info balancer.DoneInfo) {
			// 可以在这里实现请求完成后的逻辑，比如调整权重
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
			mu: &sync.RWMutex{},
			conn:           sub,
			readWeight:     readWeight,
			curReadWeight:  readWeight,
			writeWeight:    writeWeight,
			curWriteWeight: writeWeight,
		})
	}
	return &RWBalancer{
		nodes: nodes,
	}
}
