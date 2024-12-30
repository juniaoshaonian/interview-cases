package service

import (
	"context"
	"google.golang.org/grpc"
	"interview-cases/case11_20/case17/pb"
	"time"
)

type TestService struct {
	pb.UnimplementedTestServiceServer
}

func (t *TestService) Test(ctx context.Context, request *pb.TestRequest) (*pb.TestResponse, error) {
	time.Sleep(100 * time.Millisecond)
	return &pb.TestResponse{
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

func RegisterTestServiceServer(s *grpc.Server, svc *TestService) {
	pb.RegisterTestServiceServer(s, svc)
}
