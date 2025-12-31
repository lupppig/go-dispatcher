package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Dispatcher struct {
	MaxWorkers  int
	IncomingJob chan *Job
	Workers     []*Worker

	WorkerMetrics *Metrics
	MaxRetries    int
	MaxJob        int

	mu  *sync.Mutex
	DLQ []*Job

	rrIndex int

	quitChan chan bool // forceful shutdown
	stopChan chan bool // graceful shutdown

	wg *sync.WaitGroup
}

type Result struct {
	Status JobStatus
	Job    *Job
	Err    error
}

func NewDispatcher(maxWorkers int) *Dispatcher {

	return &Dispatcher{
		MaxWorkers:    maxWorkers,
		IncomingJob:   make(chan *Job),
		Workers:       make([]*Worker, 0),
		MaxJob:        20,
		mu:            new(sync.Mutex),
		DLQ:           make([]*Job, 0),
		WorkerMetrics: NewMetrics(),
		quitChan:      make(chan bool),
		stopChan:      make(chan bool),
		wg:            new(sync.WaitGroup),
	}
}

func (d *Dispatcher) startWorkers() {
	for nWorker := range d.MaxWorkers {
		w := NewWorker(nWorker, d.MaxJob, d.WorkerMetrics)
		d.Workers = append(d.Workers, w)
		go w.StartWorker()
	}
}

func (d *Dispatcher) Run() {
	d.startWorkers()
	go d.dispatchLoop()
	go d.resultLoop()
}

func (d *Dispatcher) dispatchLoop() {
	for {
		select {
		case job, ok := <-d.IncomingJob:
			if !ok {
				fmt.Println("[Dispatcher] Incoming job channel closed")
				return
			}

			dispatched := false
			for !dispatched {
				worker := d.Workers[d.rrIndex]
				d.rrIndex = (d.rrIndex + 1) % len(d.Workers)

				if atomic.LoadInt32(&worker.Busy) == 1 {
					continue
				}
				select {
				case worker.JobChannel <- job:
					fmt.Printf("[Dispatcher] Dispatched job %s to worker %d\n",
						job.ID, worker.ID)
					dispatched = true
				default:
				}
				time.Sleep(5 * time.Millisecond)
			}

		case <-d.stopChan:
			fmt.Println("[Dispatcher] Graceful shutdown initiated")
			close(d.IncomingJob)
			for _, w := range d.Workers {
				w.Stop()
			}
			fmt.Println("[Dispatcher] Graceful shutdown completed")
			return
		case <-d.quitChan:
			fmt.Println("[Dispatcher] Force shutdown initiated")
			for _, w := range d.Workers {
				w.Kill()
			}
			return
		}
	}
}

func (d *Dispatcher) resultLoop() {
	for _, w := range d.Workers {
		d.wg.Add(1)
		go func(worker *Worker) {
			defer d.wg.Done()
			for res := range worker.ResultChan {
				d.handleResult(res)
			}
		}(w)
	}
}

func (d *Dispatcher) handleResult(res Result) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if res.Status == JobSuccess {
		atomic.AddInt32(&d.WorkerMetrics.SuccessfulJobs, 1)
		return
	}

	job := res.Job
	job.Retries++

	atomic.AddInt32(&d.WorkerMetrics.FailedJobs, 1)
	atomic.AddInt32(&d.WorkerMetrics.JobRetryCount, 1)
	if job.Retries <= d.MaxRetries {
		fmt.Printf("🔁 Retrying job %s (%d/%d)\n",
			job.ID, job.Retries, d.MaxRetries)
		d.IncomingJob <- job
		return
	}

	fmt.Printf("☠️ Job %s moved to DLQ loooooooool\n", job.ID)
	d.mu.Lock()
	d.DLQ = append(d.DLQ, job)
	d.mu.Unlock()
}

func (d *Dispatcher) Close() {
	d.stopChan <- true
	d.wg.Wait()
}

func (d *Dispatcher) Kill() {
	close(d.quitChan)
}
