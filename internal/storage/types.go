package storage

import (
	"time"
)

// TaskStatus описывает состояние задачи скачивания
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskCanceled  TaskStatus = "canceled"
)

// DownloadItem описывает один файл в задаче
type DownloadItem struct {
	URL        string    `json:"url"`
	FileName   string    `json:"fileName"`
	BytesTotal int64     `json:"bytesTotal"`
	BytesDone  int64     `json:"bytesDone"`
	Error      string    `json:"error"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
}

// Task модель задачи пользователя
type Task struct {
	ID         string         `json:"id"`
	CreatedAt  time.Time      `json:"createdAt"`
	UpdatedAt  time.Time      `json:"updatedAt"`
	Status     TaskStatus     `json:"status"`
	Items      []DownloadItem `json:"items"`
	OutputDir  string         `json:"outputDir"`
	RetryCount int            `json:"retryCount"`
	MaxRetries int            `json:"maxRetries"`
}
