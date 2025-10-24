package coordinator

import (
	"context"
	"log"
	"time"

	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
)

func (s *CoordinatorServer) scanDB() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go s.scanAndSubmit()
		case <-s.ctx.Done():
			return
		}
	}

}

func (s *CoordinatorServer) scanAndSubmit() {
	// get scheduled task from db
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := s.dbPool.Begin(ctx)
	if err != nil {
		log.Println("error beginning transaction", err)
		return
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil && err.Error() != "tx is closed" {
			log.Printf("Error: %#v", err)
			log.Printf("Failed to rollback transaction. %v\n", err)
		}
	}()

	rows, err := tx.Query(ctx, `SELECT id, endpoint FROM tasks WHERE scheduled_at < (Now() + INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`)
	if err != nil {
		log.Printf("Error executing query. %v\n", err)
		return
	}

	defer rows.Close()

	var tasks []*pb.SubmitTaskRequest

	for rows.Next() {
		var id, endpoint string
		if err := rows.Scan(&id, &endpoint); err != nil {
			log.Printf("Failed to scan row. %v\n", err)
			continue
		}

		tasks = append(tasks, &pb.SubmitTaskRequest{TaskId: id, Endpoint: endpoint})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v\n", err)
		return
	}

	// s.submitTaskToWorker() for all those tasks
	for _, task := range tasks {
		if err := s.submitTaskToWorker(task); err != nil {
			log.Printf("Failed to submit task %s: %v\n", task.GetTaskId(), err)
			continue
		}

		if _, err := tx.Exec(ctx, "UPDATE tasks SET picked_at = NOW() WHERE id = $1", task.GetTaskId()); err != nil {
			log.Printf("Failed to update task %s: %v\n", task.GetTaskId(), err)
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v\n", err)
	}

}


func (s *CoordinatorServer) submitTaskToWorker (task *pb.SubmitTaskRequest) error {
	workerID, err := s.getNextWorker()
	if err != nil {
		return err
	}

	_, err = s.workerPool[workerID].workerServiceClient.SubmitTask(context.Background(), task)
	if err != nil {
		return err
	}
	return nil
}








