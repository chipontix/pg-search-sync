// Package app holds the few helpers every binary shares.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func EnvInt(key string, def int) int {
	v, err := strconv.Atoi(Env(key, strconv.Itoa(def)))
	if err != nil {
		Fatal("bad int env", "key", key, "error", err)
	}
	return v
}

func EnvDuration(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(Env(key, def.String()))
	if err != nil {
		Fatal("bad duration env", "key", key, "error", err)
	}
	return v
}

func EnvList(key, def string) []string { return strings.Split(Env(key, def), ",") }

func Fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

// SignalContext is cancelled on SIGINT/SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
}

// Serve runs an HTTP server until ctx is done.
func Serve(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	slog.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Metrics returns a registry with the Go runtime and process collectors and
// a handler serving it.
func Metrics() (*prometheus.Registry, http.Handler) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
