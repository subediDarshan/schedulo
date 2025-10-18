package scheduler

import (
	"github.com/jackc/pgtype"
)

type TaskRequest struct {
	Endpoint     string `json:"endpoint"`
	Scheduled_at string `json:"scheduled_at"`
}

type Task struct {
	Id           string `json:"task_id"`
	Endpoint     string `json:"endpoint"`
	Scheduled_at pgtype.Timestamp `json:"scheduled_at"`
	Picked_at    pgtype.Timestamp `json:"picked_at"`
	Started_at   pgtype.Timestamp `json:"started_at"`
	Completed_at pgtype.Timestamp `json:"completed_at"`
	Failed_at    pgtype.Timestamp `json:"failed_at"`
}

type TaskResponse struct {
	Id           string `json:"task_id"`
	Endpoint     string `json:"endpoint"`
	Scheduled_at string `json:"scheduled_at"`
}

