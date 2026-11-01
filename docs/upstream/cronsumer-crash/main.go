// Repro: a SIGKILL while kafka-cronsumer processes a retry record loses that
// record. ReadMessage commits the offset right after FetchMessage, before
// consumeFn runs, so the restarted group resumes after it.
//
//	go run . produce 200   # 200 records into repro.retry
//	go run . consume       # processes them, appending each key to processed.log
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	cronsumer "github.com/Trendyol/kafka-cronsumer"
	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segment "github.com/segmentio/kafka-go"
)

const topic = "repro.retry"

// REPRO_BROKER: the Kafka address (default localhost:19092, the compose Redpanda).
var broker = func() string {
	if b := os.Getenv("REPRO_BROKER"); b != "" {
		return b
	}
	return "localhost:19092"
}()

// WORK_MS: how long consumeFn takes per record (default 20ms).
var work = func() time.Duration {
	ms, err := strconv.Atoi(os.Getenv("WORK_MS"))
	if err != nil {
		ms = 20
	}
	return time.Duration(ms) * time.Millisecond
}()

func main() {
	switch os.Args[1] {
	case "produce":
		n, _ := strconv.Atoi(os.Args[2])
		conn, err := segment.Dial("tcp", broker)
		must(err)
		_ = conn.CreateTopics(segment.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
		conn.Close()
		w := &segment.Writer{Addr: segment.TCP(broker), Topic: topic}
		msgs := make([]segment.Message, n)
		for i := range msgs {
			msgs[i] = segment.Message{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("v")}
		}
		must(w.WriteMessages(context.Background(), msgs...))
		must(w.Close())
	case "consume":
		f, err := os.OpenFile("processed.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		must(err)
		cfg := &kafka.Config{
			Brokers: []string{broker},
			Consumer: kafka.ConsumerConfig{
				GroupID: "repro", Topic: topic, MaxRetry: 3, Concurrency: 1,
				Cron: "@every 1s", Duration: kafka.NonStopWork,
			},
			LogLevel: "warn",
		}
		c := cronsumer.New(cfg, func(m kafka.Message) error {
			time.Sleep(work) // the work, e.g. an Elasticsearch write
			_, err := f.WriteString(string(m.Key) + "\n")
			if err == nil {
				err = f.Sync()
			}
			return err
		})
		c.Run()
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
