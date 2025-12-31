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
	go d.autoScaleLoop()
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
	if res.Status == JobSuccess {
		atomic.AddInt32(&d.WorkerMetrics.SuccessfulJobs, 1)
		return
	}

	job := res.Job
	job.Retries++

	if job.Retries <= d.MaxRetries {
		atomic.AddInt32(&d.WorkerMetrics.JobRetryCount, 1)

		delay := time.Duration(1<<uint(job.Retries-1)) * time.Second
		fmt.Printf("🔁 Retrying job %s (%d/%d) in %v\n", job.ID, job.Retries, d.MaxRetries, delay)

		go func(j *Job, delay time.Duration) {
			time.Sleep(delay)
			select {
			case d.IncomingJob <- j:
			default:
				fmt.Printf("⚠️ Retry queue full, moving job %s to DLQ\n", j.ID)
				d.mu.Lock()
				d.DLQ = append(d.DLQ, j)
				d.mu.Unlock()
			}
		}(job, delay)

		return
	}

	atomic.AddInt32(&d.WorkerMetrics.FailedJobs, 1)
	fmt.Printf("☠️ Job %v moved to DLQ after %d retries lmaooooooo\n", job.Name, job.Retries-1)
	d.mu.Lock()
	d.DLQ = append(d.DLQ, job)
	d.mu.Unlock()
}

func (d *Dispatcher) scaleDown() {
	if len(d.Workers) <= 1 {
		return
	}

	for i := len(d.Workers) - 1; i >= 0; i-- {
		w := d.Workers[i]
		if atomic.LoadInt32(&w.Busy) == 0 {
			w.Stop() // graceful
			d.Workers = append(d.Workers[:i], d.Workers[i+1:]...)
			fmt.Printf("[Autoscaler] scaled down → %d workers\n", len(d.Workers))
			return
		}
	}
}

func (d *Dispatcher) scaleUp() {
	if len(d.Workers) >= d.MaxWorkers {
		return
	}

	id := len(d.Workers)
	w := NewWorker(id, d.MaxJob, d.WorkerMetrics)

	d.Workers = append(d.Workers, w)
	go w.StartWorker()

	fmt.Printf("[Autoscaler] scaled up → %d workers\n", len(d.Workers))
}

func (d *Dispatcher) autoScaleLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			busy := 0
			for _, w := range d.Workers {
				if atomic.LoadInt32(&w.Busy) == 1 {
					busy++
				}
			}

			total := len(d.Workers)
			queueLen := len(d.IncomingJob)

			utilization := float64(busy) / float64(total)

			if utilization >= 0.8 && queueLen > total {
				d.mu.Lock()
				d.scaleUp()
				d.mu.Unlock()
			}

			if utilization <= 0.3 && queueLen == 0 {
				d.mu.Lock()
				d.scaleDown()
				d.mu.Unlock()
			}

		case <-d.stopChan:
			return
		case <-d.quitChan:
			return
		}
	}
}

func (d *Dispatcher) Close() {
	d.stopChan <- true
	d.wg.Wait()
}

func (d *Dispatcher) Kill() {
	close(d.quitChan)
}
