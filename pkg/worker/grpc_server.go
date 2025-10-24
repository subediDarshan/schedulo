package worker

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
	"google.golang.org/grpc"
)

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.serverPort == "" {
		// Find a free port using a temporary socket
		w.listener, err = net.Listen("tcp", ":0")                                // Bind to any available port
		w.serverPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port) // Get the assigned port
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.serverPort, err)
	}

	log.Printf("Starting worker server on %s\n", w.serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	log.Printf("Received task: Task ID: %v, Endpoint: %v", req.GetTaskId(), req.GetEndpoint())

	w.ReceivedTasksMutex.Lock()
	w.ReceivedTasks[req.GetTaskId()] = req
	w.ReceivedTasksMutex.Unlock()

	w.taskQueue <- req

	return &pb.SubmitTaskResponse{
		Success: true,
	}, nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Error while closing the listener: %v", err)
		}
	}

	if err := w.coordinatorConnection.Close(); err != nil {
		log.Printf("Error while closing client connection with coordinator: %v", err)
	}
}
