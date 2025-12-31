package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// type WorkerSlot struct {
// 	WorkerId   int
// 	JobChannel *Job
// }

type Worker struct {
	ID            int
	JobChannel    chan *Job
	StopChan      chan bool
	ResultChan    chan Result
	QuitChan      chan bool
	Busy          int32
	WorkerMetrics *Metrics
	wLock         *sync.Mutex
	wg            *sync.WaitGroup
}

func NewWorker(id int, jcSize int, metrics *Metrics) *Worker {
	return &Worker{
		ID:            id,
		JobChannel:    make(chan *Job, jcSize),
		StopChan:      make(chan bool),
		QuitChan:      make(chan bool),
		wLock:         new(sync.Mutex),
		ResultChan:    make(chan Result),
		wg:            new(sync.WaitGroup),
		WorkerMetrics: metrics,
	}
}

func (w *Worker) StartWorker() {
	defer close(w.ResultChan)
	for {
		select {
		case job, ok := <-w.JobChannel:
			if !ok {
				fmt.Printf("[Worker %d] Job channel closed\n", w.ID)
				return
			}
			atomic.StoreInt32(&w.Busy, 1)
			fmt.Printf("[Worker %d] Processing job %s\n", w.ID, job.ID)

			atomic.AddInt32(&w.WorkerMetrics.TotalJobs, 1)
			status, err := processJob(*job, w.ID)

			w.ResultChan <- Result{Job: job, Status: status, Err: err}
			atomic.StoreInt32(&w.Busy, 0)

		case <-w.QuitChan:
			fmt.Printf("[Worker %d] Force killing worker\n", w.ID)
			return
		case <-w.StopChan:
			fmt.Printf("[Worker %d] Graceful stop initiated\n", w.ID)

			for atomic.LoadInt32(&w.Busy) == 1 {
				time.Sleep(10 * time.Millisecond)
			}
			fmt.Printf("[Worker %d] Graceful stop completed\n", w.ID)
			return
		}
	}
}

func (w *Worker) Stop() {
	select {
	case w.StopChan <- true:
	default:
	}
}

func (w *Worker) Kill() {
	close(w.QuitChan)
}
