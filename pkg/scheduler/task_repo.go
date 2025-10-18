package scheduler

import "context"

func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	sqlQuery := "INSERT INTO tasks (endpoint, scheduled_at) VALUES ($1, $2) RETURNING id"

	var insertedTaskId string

	err := s.dbPool.QueryRow(ctx, sqlQuery, task.Endpoint, task.Scheduled_at).Scan(&insertedTaskId)
	if err != nil {
		return "", err
	}

	return insertedTaskId, nil

}

func (s *SchedulerServer) getTaskStatusFromDB(ctx context.Context, taskId string) (Task, error) {
	sqlQuery := "SELECT * FROM tasks WHERE id = $1"

	var task Task

	err := s.dbPool.QueryRow(ctx, sqlQuery, taskId).Scan(&task.Id, &task.Endpoint, &task.Scheduled_at, &task.Picked_at, &task.Started_at, &task.Completed_at, &task.Failed_at)
	if err != nil {
		return Task{}, err
	}

	return task, nil
}
