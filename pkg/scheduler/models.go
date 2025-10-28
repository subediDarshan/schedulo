package scheduler

import (
	"encoding/json"

	"github.com/jackc/pgtype"
)

type TaskRequest struct {
	Endpoint     string          `json:"endpoint"`
	Scheduled_at string          `json:"scheduled_at"`
	Bearer_Token string          `json:"bearer_token"`
	Method       string          `json:"method"`
	Payload      json.RawMessage `json:"payload"`
}

type Task struct {
	Id           string           `json:"task_id"`
	Endpoint     string           `json:"endpoint"`
	Bearer_Token string           `json:"bearer_token"`
	Scheduled_at pgtype.Timestamp `json:"scheduled_at"`
	Method       string           `json:"method"`
	Payload      pgtype.Bytea     `json:"payload"`
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
