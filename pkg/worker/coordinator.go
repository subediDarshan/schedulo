package worker

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	var err error
	w.coordinatorConnection, err = grpc.NewClient(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorConnection)
	log.Println("Connected to coordinator!")
	return nil
}

func (w *WorkerServer) periodicHeartbeat() {
	w.wg.Add(1)      
	defer w.wg.Done() 

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// Fall back to using the listener address if WORKER_ADDRESS is not set
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.serverPort
	}

	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) updateTaskStatus(task *pb.SubmitTaskRequest, status pb.TaskStatus) {
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskId:      task.GetTaskId(),
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
		FailedAt:    time.Now().Unix(),
	})
}
