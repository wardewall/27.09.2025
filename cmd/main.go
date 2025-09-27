package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"27.09.2025/internal/api"
	"27.09.2025/internal/service"
	"27.09.2025/internal/storage"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	// Конфигурация
	baseDir := getenv("APP_DATA_DIR", filepath.Join(".", "data"))
	addr := getenv("APP_HTTP_ADDR", ":9091")

	// Инициализация хранилища
	store, err := storage.NewFileStore(baseDir)
	if err != nil {
		log.Fatalf("init store: %v", err)
	}

	// Инициализация менеджера
	mgr, err := service.NewManager(store, service.Config{OutputDir: filepath.Join(baseDir, "downloads")})
	if err != nil {
		log.Fatalf("init manager: %v", err)
	}

	// Контекст для воркеров
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		log.Fatalf("start manager: %v", err)
	}

	// HTTP сервер
	srv := api.NewServer(addr, mgr)
	if err := srv.Start(); err != nil {
		log.Fatalf("start http: %v", err)
	}
	log.Printf("listening on %s", addr)

	// Ожидаем сигнал завершения
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")

	// Сначала останавливаем воркеров, оставляя HTTP доступным для приёма новых задач
	cancel()
	mgr.Stop()

	// Затем аккуратно останавливаем HTTP
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)

	log.Println("bye")
}
