package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	return &Metrics{
		SuccessfulJobs: 0,
		FailedJobs:     0,
		TotalJobs:      0,
		JobCount:       0,
		ActiveWorkers:  0,
		JobRetryCount:  0,
	}
}

var ErrorFailedToProcessJob = errors.New("failed to process job lmaoooooooooooooo")

// jobs containing numerals are considered successful else failed
func processJob(notif Job, workerId int) (JobStatus, error) {
	time.Sleep(time.Duration(workerId) * time.Millisecond) // simulate different job loads

	messageId := strings.Split(notif.Name, " ")
	if len(messageId) < 2 {
		fmt.Printf("❌ Failed to process job!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n----------------\n", notif.Name, notif.ID, "Failed")
		return JobFailure, ErrorFailedToProcessJob
	}

	_, err := strconv.Atoi(messageId[1])
	if err != nil {
		fmt.Printf("❌ Failed to process job!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n----------------\n", notif.Name, notif.ID, "Failed")
		return JobFailure, ErrorFailedToProcessJob
	}
	fmt.Printf("✅ Job processed successfully!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n📦 Payload: %+v\n----------------\n", notif.Name, notif.ID, "Success", notif.Payload)
	return JobSuccess, nil
}
