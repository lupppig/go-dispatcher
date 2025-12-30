package main

import (
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
}

func NewJob(payload interface{}, name string, maxRetries int) *Job {
	return &Job{
		ID:      uuid.New().String(),
		Name:    name,
		Payload: payload,
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

// jobs containing numerals are considered successful else failed
func SendNotification(notif Job, workerId int) JobStatus {
	time.Sleep(time.Duration(workerId) * time.Millisecond) // simulate different job loads

	messageId := strings.Split(notif.Name, " ")
	if len(messageId) < 2 {
		fmt.Printf("❌ Failed to process job!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n----------------\n", notif.Name, notif.ID, "Failed")
		return JobFailure
	}

	_, err := strconv.Atoi(messageId[1])
	if err != nil {
		fmt.Printf("❌ Failed to process job!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n----------------\n", notif.Name, notif.ID, "Failed")
		return JobFailure
	}
	fmt.Printf("✅ Job processed successfully!\n📝 Name: %v\n🆔 ID: %v\n📌 Status: %v\n📦 Payload: %+v\n----------------\n", notif.Name, notif.ID, "Success", notif.Payload)
	return JobSuccess
}
