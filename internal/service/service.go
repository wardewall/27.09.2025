package service

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"27.09.2025/internal/storage"
)

// Manager управляет очередью задач и воркерами
type Manager struct {
	store     storage.Store
	outputDir string
	queueCh   chan string
	workers   int
	client    *http.Client
	wg        sync.WaitGroup
	mu        sync.Mutex
	started   bool
}

type Config struct {
	OutputDir      string
	ConcurrentJobs int
	HTTPTimeout    time.Duration
}

func NewManager(store storage.Store, cfg Config) (*Manager, error) {
	if cfg.ConcurrentJobs <= 0 {
		cfg.ConcurrentJobs = runtime.NumCPU()
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 60 * time.Second
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = filepath.Join(store.DataDir(), "downloads")
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	m := &Manager{
		store:     store,
		outputDir: cfg.OutputDir,
		queueCh:   make(chan string, 1024),
		workers:   cfg.ConcurrentJobs,
		client:    &http.Client{Timeout: cfg.HTTPTimeout},
	}
	return m, nil
}

// Start запускает воркеров и перезапланирует незавершённые задачи
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = true
	m.mu.Unlock()

	// Восстановление: перечитать все задачи и отправить pending в очередь
	tasks, err := m.store.ListTasks()
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if t.Status == storage.TaskPending {
			m.Enqueue(t.ID)
		}
	}

	// Запуск воркеров
	for i := 0; i < m.workers; i++ {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.worker(ctx)
		}()
	}
	return nil
}

// Stop ожидает завершения воркеров (graceful)
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// Enqueue добавляет задачу в очередь по ID
func (m *Manager) Enqueue(taskID string) {
	select {
	case m.queueCh <- taskID:
	default:
		// если очередь заполнена — блокируемся
		m.queueCh <- taskID
	}
}

// CreateTask создаёт новую задачу и ставит в очередь
func (m *Manager) CreateTask(urls []string) (*storage.Task, error) {
	if len(urls) == 0 {
		return nil, errors.New("empty urls")
	}
	items := make([]storage.DownloadItem, 0, len(urls))
	for _, u := range urls {
		items = append(items, storage.DownloadItem{URL: strings.TrimSpace(u)})
	}
	id := generateTaskID(urls)
	t := &storage.Task{
		ID:         id,
		Status:     storage.TaskPending,
		Items:      items,
		OutputDir:  filepath.Join(m.outputDir, id),
		MaxRetries: 2,
	}
	if err := m.store.CreateTask(t); err != nil {
		return nil, err
	}
	m.Enqueue(t.ID)
	return t, nil
}

func generateTaskID(urls []string) string {
	h := sha1.Sum([]byte(strings.Join(urls, "|")))
	return hex.EncodeToString(h[:])
}

func (m *Manager) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-m.queueCh:
			if !ok {
				return
			}
			m.processTask(ctx, id)
		}
	}
}

func (m *Manager) processTask(ctx context.Context, id string) {
	t, err := m.store.GetTask(id)
	if err != nil {
		return
	}
	if t.Status != storage.TaskPending {
		return
	}
	t.Status = storage.TaskRunning
	if err := m.store.SaveTask(t); err != nil {
		return
	}
	if err := os.MkdirAll(t.OutputDir, 0o755); err != nil {
		m.failTask(t, fmt.Errorf("mkdir output: %w", err))
		return
	}
	var firstErr error
	for i := range t.Items {
		if err := m.downloadOne(ctx, t, &t.Items[i]); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
		_ = m.store.SaveTask(t)
	}
	if firstErr != nil {
		t.RetryCount++
		if t.RetryCount <= t.MaxRetries {
			t.Status = storage.TaskPending
			_ = m.store.SaveTask(t)
			// планируем повтор
			time.AfterFunc(3*time.Second, func() { m.Enqueue(t.ID) })
			return
		}
		m.failTask(t, firstErr)
		return
	}
	t.Status = storage.TaskCompleted
	_ = m.store.SaveTask(t)
}

func (m *Manager) failTask(t *storage.Task, err error) {
	t.Status = storage.TaskFailed
	if len(t.Items) > 0 && t.Items[0].Error == "" {
		// фиксируем общую ошибку в первом элементе, если нет частных
		t.Items[0].Error = err.Error()
	}
	_ = m.store.SaveTask(t)
}

func (m *Manager) downloadOne(ctx context.Context, t *storage.Task, item *storage.DownloadItem) error {
	u := item.URL
	parsed, err := url.Parse(u)
	if err != nil {
		item.Error = fmt.Sprintf("bad url: %v", err)
		return err
	}
	fileName := item.FileName
	if fileName == "" {
		fileName = filepath.Base(parsed.Path)
		if fileName == "" || fileName == "/" || fileName == "." {
			fileName = sanitizeName(parsed.Host)
		}
		item.FileName = fileName
	}
	item.StartedAt = time.Now().UTC()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		item.Error = err.Error()
		return err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		item.Error = err.Error()
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		item.Error = fmt.Sprintf("http %d", resp.StatusCode)
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		item.BytesTotal = resp.ContentLength
	}
	outPath := filepath.Join(t.OutputDir, fileName)
	f, err := os.Create(outPath)
	if err != nil {
		item.Error = err.Error()
		return err
	}
	defer f.Close()

	n, err := io.Copy(f, io.TeeReader(resp.Body, &byteCounter{onWrite: func(n int) {
		item.BytesDone += int64(n)
	}}))
	if err != nil {
		item.Error = err.Error()
		return err
	}
	item.BytesDone = n
	item.FinishedAt = time.Now().UTC()
	return nil
}

type byteCounter struct{ onWrite func(int) }

func (b *byteCounter) Write(p []byte) (int, error) {
	if b.onWrite != nil {
		b.onWrite(len(p))
	}
	return len(p), nil
}

func sanitizeName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "..", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	if s == "" {
		s = "file"
	}
	return s
}

func (m *Manager) GetTask(id string) (*storage.Task, error) { return m.store.GetTask(id) }
func (m *Manager) ListTasks() ([]*storage.Task, error)      { return m.store.ListTasks() }
