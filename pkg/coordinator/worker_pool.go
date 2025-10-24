package coordinator

import (
	"fmt"
	"log"
	"time"
)

func (s *CoordinatorServer) manageWorkerPool() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(s.maxHeartbeatMisses) * s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeInactiveWorkers()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	for workerId, worker := range s.workerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", workerId)
			// remove from worker pool
			worker.grpcConnection.Close()
			delete(s.workerPool, workerId)

			// remove from worker pool keys
			s.workerPoolKeysMutex.Lock()

			workerCount := len(s.workerPool)
			s.workerPoolKeys = make([]uint32, 0, workerCount)
			for k := range s.workerPool {
				s.workerPoolKeys = append(s.workerPoolKeys, k)
			}

			s.workerPoolKeysMutex.Unlock()

		} else {
			worker.heartbeatMisses++
		}
	}
}


func (s *CoordinatorServer) getNextWorker() (uint32, error) {
	s.workerPoolKeysMutex.RLock()
	defer s.workerPoolKeysMutex.RUnlock()

	workerCount := len(s.workerPoolKeys)
	if workerCount == 0 {
		return 0, fmt.Errorf("no available workers")
	}

	workerID := s.workerPoolKeys[(s.roundRobinIndex)%(uint32(workerCount))]

	s.roundRobinIndex++

	return workerID, nil

}
