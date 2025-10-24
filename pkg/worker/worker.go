package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	pb "github.com/subediDarshan/schedulo/pkg/grpcapi"
)

func (w *WorkerServer) startWorkerPool(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
}

func (w *WorkerServer) worker() {
	defer w.wg.Done() // Signal this worker is done when the function returns.

	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			err := w.processTask(task)
			if err != nil {
				continue
			}
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETED)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) processTask(task *pb.SubmitTaskRequest) error {
	log.Printf("Processing task: Task ID: %v, Endpoint: %v", task.GetTaskId(), task.GetEndpoint())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, task.GetEndpoint(), nil)
	if err != nil {
		log.Printf("Failed to create request for task %s: %v", task.GetTaskId(), err)
		go w.updateTaskStatus(task, pb.TaskStatus_FAILED)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer " + task.GetCronSecret())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to execute task %s: %v", task.GetTaskId(), err)
		go w.updateTaskStatus(task, pb.TaskStatus_FAILED)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Task %s returned error status: %s", task.GetTaskId(), resp.Status)
		go w.updateTaskStatus(task, pb.TaskStatus_FAILED)
		return fmt.Errorf("task %s failed with status code %d", task.GetTaskId(), resp.StatusCode)
	}

	log.Printf("Task %s executed. Status: %s", task.GetTaskId(), resp.Status)

	log.Printf("Completed task: %+v", task)

	return nil
}
