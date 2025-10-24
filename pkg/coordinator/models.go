package coordinator

import (
	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
	"google.golang.org/grpc"
)

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}
