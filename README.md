# Go Job Dispatcher

A Go project to learn concurrency by implementing a scalable job dispatcher with retry logic, auto-scaling workers, and metrics logging.

---

## Project Overview

This project demonstrates efficient handling of thousands of concurrent jobs using Go's goroutines, channels, and atomic operations. It is designed to:

- Dispatch jobs to multiple workers using a round-robin algorithm.
- Retry failed jobs with exponential backoff.
- Move jobs that fail after maximum retries to a Dead-Letter Queue (DLQ).
- Automatically scale workers up or down based on job queue size and worker utilization.
- Log job metrics periodically including successful jobs, failed jobs, retries, total jobs, and active workers.
- Support graceful and forceful shutdowns.

---

## Features

- Round-robin job dispatching
- Job retry logic with exponential backoff
- Dead-letter queue for failed jobs
- Auto-scaling workers
- Metrics logging
- Graceful and forceful shutdown support
- Can handle thousands of concurrent jobs

---

## Diagram


                     ┌───────────────────────┐
                     │      Dispatcher       │
                     │  (Round-Robin Logic) │
                     └─────────┬────────────┘
                               │
                               │
                               ▼
                    ┌───────────────────┐
                    │ Incoming Job Queue│
                    └─────────┬─────────┘
                              │
                              ▼
             ┌───────────┬───────────┬───────────┐
             │ Worker 1  │ Worker 2  │ Worker 3  │
             │ (Busy?)   │ (Busy?)   │ (Busy?)   │
             └─────┬─────┴─────┬─────┴─────┬─────┘
                   │           │           │
                   ▼           ▼           ▼
                Job Success / Retry / Failure
                   │
                   ▼
           ┌────────────────────┐
           │ Dead-Letter Queue  │
           └────────────────────┘


