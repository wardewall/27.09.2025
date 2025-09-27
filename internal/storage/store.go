package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrTaskNotFound = errors.New("task not found")
)

// Store определяет интерфейс персистентного хранилища задач
type Store interface {
	CreateTask(task *Task) error
	SaveTask(task *Task) error
	GetTask(id string) (*Task, error)
	ListTasks() ([]*Task, error)
	DataDir() string
}

// FileStore — файловая реализация Store
type FileStore struct {
	baseDir  string
	tasksDir string
	mu       sync.RWMutex
	inMemory map[string]*Task
}

// NewFileStore инициализирует файловое хранилище под указанной директорией
func NewFileStore(baseDir string) (*FileStore, error) {
	tasksDir := filepath.Join(baseDir, "tasks")
	if err := os.MkdirAll(tasksDir, 0o755); err != nil {
		return nil, fmt.Errorf("create tasks dir: %w", err)
	}
	fs := &FileStore{
		baseDir:  baseDir,
		tasksDir: tasksDir,
		inMemory: make(map[string]*Task),
	}
	if err := fs.loadAll(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (s *FileStore) DataDir() string { return s.baseDir }

func (s *FileStore) taskPath(id string) string {
	return filepath.Join(s.tasksDir, id+".json")
}

// loadAll загружает все задачи из каталога.
// Задачи в статусе running переводятся в pending для повторной постановки в очередь.
func (s *FileStore) loadAll() error {
	entries, err := os.ReadDir(s.tasksDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read tasks dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		path := filepath.Join(s.tasksDir, e.Name())
		b, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read task file %s: %w", path, err)
		}
		var t Task
		if err := json.Unmarshal(b, &t); err != nil {
			return fmt.Errorf("unmarshal task %s: %w", path, err)
		}
		// Нормализуем статусы после рестарта
		if t.Status == TaskRunning {
			t.Status = TaskPending
			t.UpdatedAt = time.Now().UTC()
		}
		s.inMemory[t.ID] = &t
	}
	return nil
}

func (s *FileStore) CreateTask(task *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.inMemory[task.ID]; exists {
		return fmt.Errorf("task %s already exists", task.ID)
	}
	now := time.Now().UTC()
	task.CreatedAt = now
	task.UpdatedAt = now
	if task.Status == "" {
		task.Status = TaskPending
	}
	if task.MaxRetries == 0 {
		task.MaxRetries = 2
	}
	if err := writeJSONAtomic(s.taskPath(task.ID), task); err != nil {
		return err
	}
	copied := *task
	s.inMemory[task.ID] = &copied
	return nil
}

func (s *FileStore) SaveTask(task *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.inMemory[task.ID]; !exists {
		return ErrTaskNotFound
	}
	task.UpdatedAt = time.Now().UTC()
	if err := writeJSONAtomic(s.taskPath(task.ID), task); err != nil {
		return err
	}
	copied := *task
	s.inMemory[task.ID] = &copied
	return nil
}

func (s *FileStore) GetTask(id string) (*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.inMemory[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	copied := *t
	return &copied, nil
}

func (s *FileStore) ListTasks() ([]*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Task, 0, len(s.inMemory))
	for _, t := range s.inMemory {
		copied := *t
		result = append(result, &copied)
	}
	return result, nil
}

// writeJSONAtomic выполняет атомарную запись JSON через временный файл и rename
func writeJSONAtomic(path string, v any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("encode json: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename tmp: %w", err)
	}
	return nil
}
