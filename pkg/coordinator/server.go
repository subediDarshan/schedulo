package coordinator

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/subediDarshan/schedulo/pkg/common"
	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
	"google.golang.org/grpc"
)

const (
	defaultMaxHeartbeatMisses = 1
	defaultScanInterval       = 10 * time.Second
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	serverPort          string
	listener            net.Listener
	grpcServer          *grpc.Server
	dbConnectionString  string
	dbPool              *pgxpool.Pool
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	workerPool          map[uint32]*workerInfo
	workerPoolMutex     sync.Mutex
	workerPoolKeys      []uint32
	workerPoolKeysMutex sync.RWMutex
	roundRobinIndex     uint32
	maxHeartbeatMisses  uint8
	heartbeatInterval   time.Duration
	scanInterval        time.Duration
}

func NewServer(serverPort, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		serverPort:         serverPort,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
		workerPool:         make(map[uint32]*workerInfo),
		maxHeartbeatMisses: defaultMaxHeartbeatMisses,
		heartbeatInterval:  common.DefaultHeartbeatInterval,
		scanInterval:       defaultScanInterval,
	}
}

func (s *CoordinatorServer) Start() error {
	// manage worker pool
	go s.manageWorkerPool()

	// start grpc server
	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("error starting Coordinator GRPC Server. %w", err)
	}

	// db connect
	var err error
	s.dbPool, err = common.ConnectToDB(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	// scan and submit task
	go s.scanDB()

	// return s.awaitShutdown()
	return s.awaitShutdown()
}



func (s *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop

	return s.stop()
}

func (s *CoordinatorServer) stop() error {
	s.cancel()
	s.wg.Wait()

	s.dbPool.Close()

	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()
	for _, worker := range s.workerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	return nil

}
