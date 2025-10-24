package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *CoordinatorServer) startGRPCServer() error {
	var err error

	s.listener, err = net.Listen("tcp", s.serverPort)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Fatalf("Coordinator GRPC Server failed. %v", err)
		}
	}()

	return nil
}

// rpcs initialization

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	workerID := in.GetWorkerId()

	if worker, ok := s.workerPool[workerID]; ok {
		worker.heartbeatMisses = 0
	} else {
		log.Printf("Registering worker with id: %v\n", workerID)
		conn, err := grpc.NewClient(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		s.workerPool[workerID] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		s.workerPoolKeysMutex.Lock()
		defer s.workerPoolKeysMutex.Unlock()

		workerCount := len(s.workerPool)

		s.workerPoolKeys = make([]uint32, 0, workerCount)

		for workerID := range s.workerPool {
			s.workerPoolKeys = append(s.workerPoolKeys, workerID)
		}

		log.Println("Registered worker:", workerID)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := req.GetStatus()
	taskId := req.GetTaskId()
	var timestamp time.Time
	var column string

	switch status {
	case pb.TaskStatus_STARTED:
		timestamp = time.Unix(req.GetStartedAt(), 0)
		column = "started_at"
	case pb.TaskStatus_COMPLETED:
		timestamp = time.Unix(req.GetCompletedAt(), 0)
		column = "completed_at"
	case pb.TaskStatus_FAILED:
		timestamp = time.Unix(req.GetFailedAt(), 0)
		column = "failed_at"
	default:
		log.Println("Invalid Status in UpdateStatusRequest")
		return nil, errors.ErrUnsupported
	}

	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)
	_, err := s.dbPool.Exec(ctx, sqlStatement, timestamp, taskId)
	if err != nil {
		log.Printf("Could not update task status for task %s: %+v", taskId, err)
		return nil, err
	}

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}
