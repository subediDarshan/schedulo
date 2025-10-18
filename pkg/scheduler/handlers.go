package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgtype"
)

func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST Method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var taskRequest TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&taskRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	log.Printf("Received schedule request: %+v", taskRequest)

	parsedTime, err := time.Parse(time.RFC3339, taskRequest.Scheduled_at)
	if err != nil {
		http.Error(w, "Invalid date format. Use ISO 8601 format", http.StatusBadRequest)
		return
	}

	utcTime := parsedTime.UTC()

	taskId, err := s.insertTaskIntoDB(context.Background(), Task{Endpoint: taskRequest.Endpoint, Scheduled_at: pgtype.Timestamp{Time: utcTime, Status: pgtype.Present}})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to store task in DB. %s", err.Error()), http.StatusInternalServerError)
		return
	}

	taskResponse := TaskResponse{
		Id:           taskId,
		Endpoint:     taskRequest.Endpoint,
		Scheduled_at: taskRequest.Scheduled_at,
	}

	jsonResponse, err := json.Marshal(taskResponse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)

}

func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET request is allowed", http.StatusMethodNotAllowed)
		return
	}

	taskId := r.URL.Query().Get("task_id")

	if taskId == "" {
		http.Error(w, "Task Id not provided", http.StatusBadRequest)
		return
	}

	status, err := s.getTaskStatusFromDB(context.Background(), taskId)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting task status from DB. %s", err.Error()), http.StatusInternalServerError)
		return
	}

	jsonResponse, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)

}
