package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/subediDarshan/schedulo/pkg/common"
	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
	"google.golang.org/grpc"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	serverPort               string
	coordinatorAddress       string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorConnection    *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.SubmitTaskRequest
	ReceivedTasks            map[string]*pb.SubmitTaskRequest
	ReceivedTasksMutex       sync.Mutex
	ctx                      context.Context    // The root context for all goroutines
	cancel                   context.CancelFunc // Function to cancel the context
	wg                       sync.WaitGroup     // WaitGroup to wait for all goroutines to finish
}

func NewServer(port string, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  common.DefaultHeartbeatInterval,
		taskQueue:          make(chan *pb.SubmitTaskRequest, 100), // Buffered channel
		ReceivedTasks:      make(map[string]*pb.SubmitTaskRequest),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (w *WorkerServer) Start() error {
	w.startWorkerPool(workerPoolSize)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	go w.periodicHeartbeat()

	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return w.awaitShutdown()
}

func (w *WorkerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return w.Stop()
}

func (w *WorkerServer) Stop() error {
	w.cancel()
	w.wg.Wait()

	w.closeGRPCConnection()
	log.Println("Worker server stopped")
	return nil
}
