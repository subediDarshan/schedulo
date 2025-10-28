package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	log.Printf("Processing task: Task ID: %v, Endpoint: %v, Method: %v",
		task.GetTaskId(), task.GetEndpoint(), task.GetMethod())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var body io.Reader
	if len(task.GetPayload()) > 0 && task.GetMethod() != http.MethodGet {
		body = bytes.NewReader(task.GetPayload())
	}

	req, err := http.NewRequestWithContext(ctx, task.GetMethod(), task.GetEndpoint(), body)
	if err != nil {
		log.Printf("Failed to create request for task %s: %v", task.GetTaskId(), err)
		go w.updateTaskStatus(task, pb.TaskStatus_FAILED)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if token := task.GetBearerToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

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

	return nil
}
