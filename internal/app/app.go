// Package app holds the few helpers every binary shares.
package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
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
