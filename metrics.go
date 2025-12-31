package main

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID      string // a dummy job ID
	Name    string
	Payload interface{}
	Attempt int
	Retries int
}

func NewJob(payload interface{}, name string, maxRetries int) *Job {
	return &Job{
		ID:      uuid.New().String(),
		Name:    name,
		Payload: payload,
		Attempt: 0,
		Retries: maxRetries,
	}
}

type JobStatus int

const (
	JobSuccess JobStatus = iota
	JobFailure
)

type Metrics struct {
	SuccessfulJobs int32
	FailedJobs     int32
	TotalJobs      int32
	JobCount       int32
	ActiveWorkers  int32
	JobRetryCount  int32
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

var ErrorFailedToProcessJob = errors.New("failed to process job lmaoooooooooooooo")

// jobs containing numerals are considered successful else failed
func processJob(notif Job, workerId int) (JobStatus, error) {
	time.Sleep(time.Duration(workerId) * time.Millisecond) // simulate different job loads

	messageId := strings.Split(notif.Name, " ")
	if len(messageId) < 2 {
		logger.Printf("❌ Failed to process job!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n----------------\n", notif.Name, notif.ID, "Failed")
		return JobFailure, ErrorFailedToProcessJob
	}

	_, err := strconv.Atoi(messageId[1])
	if err != nil {
		logger.Printf("❌ Failed to process job!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n----------------\n", notif.Name, notif.ID, "Failed")
		return JobFailure, ErrorFailedToProcessJob
	}
	logger.Printf("✅ Job processed successfully! Name: %v, ID: %v, Payload: %+v\n",
		notif.Name, notif.ID, notif.Payload)
	return JobSuccess, nil
}

func (m *Metrics) IncSuccessful() {
	atomic.AddInt32(&m.SuccessfulJobs, 1)
}

func (m *Metrics) IncFailed() {
	atomic.AddInt32(&m.FailedJobs, 1)
}

func (m *Metrics) IncTotal() {
	atomic.AddInt32(&m.TotalJobs, 1)
}

func (m *Metrics) IncJobRetry() {
	atomic.AddInt32(&m.JobRetryCount, 1)
}

func (m *Metrics) IncActiveWorkers() {
	atomic.AddInt32(&m.ActiveWorkers, 1)
}

func (m *Metrics) DecActiveWorkers() {
	atomic.AddInt32(&m.ActiveWorkers, -1)
}

func (m *Metrics) IncJobCount() {
	atomic.AddInt32(&m.JobCount, 1)
}
func (m *Metrics) DecJobCount() {
	atomic.AddInt32(&m.JobCount, -1)
}

func (m *Metrics) GetSnapshot() Metrics {
	return Metrics{
		SuccessfulJobs: atomic.LoadInt32(&m.SuccessfulJobs),
		FailedJobs:     atomic.LoadInt32(&m.FailedJobs),
		TotalJobs:      atomic.LoadInt32(&m.TotalJobs),
		JobCount:       atomic.LoadInt32(&m.JobCount),
		ActiveWorkers:  atomic.LoadInt32(&m.ActiveWorkers),
		JobRetryCount:  atomic.LoadInt32(&m.JobRetryCount),
	}
}

func (m *Metrics) LogMetrics() {
	snapshot := m.GetSnapshot()
	logger.Printf("📊 Metrics Snapshot → SuccessfulJobs: %d, FailedJobs: %d, TotalJobs: %d, ActiveWorkers: %d, JobRetryCount: %d, JobCount: %d\n",
		snapshot.SuccessfulJobs,
		snapshot.FailedJobs,
		snapshot.TotalJobs,
		snapshot.ActiveWorkers,
		snapshot.JobRetryCount,
		snapshot.JobCount,
	)

}
