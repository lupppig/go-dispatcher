package main

import (
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
		mu:            new(sync.Mutex),
		DLQ:           make([]*Job, 0),
		WorkerMetrics: NewMetrics(),
		quitChan:      make(chan bool, 1),
		stopChan:      make(chan bool, 1),
		wg:            new(sync.WaitGroup),
	}
}

func (d *Dispatcher) startWorkers() {
	for nWorker := 0; nWorker < d.MaxWorkers; nWorker++ {
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

	go d.StartMetricsLogger(50 * time.Millisecond)

}

func (d *Dispatcher) dispatchLoop() {
	for {
		select {
		case job, ok := <-d.IncomingJob:
			if !ok {
				logger.Println("[Dispatcher] Incoming job channel closed")
				return
			}

			dispatched := false
			attempts := 0
			for !dispatched && attempts < len(d.Workers) {
				worker := d.getNextWorker()
				if worker == nil {
					break
				}

				if atomic.LoadInt32(&worker.Busy) == 0 {
					select {
					case worker.JobChannel <- job:
						logger.Printf("[Dispatcher] Dispatched job %s to worker %d\n", job.ID, worker.ID)
						atomic.AddInt32(&d.WorkerMetrics.JobCount, 1)
						dispatched = true
					default:
						// worker channel full, try next
					}
				}

				attempts++
				time.Sleep(2 * time.Millisecond) // small backoff
			}

			if !dispatched {
				// all workers busy, enqueue with delay in separate goroutine
				go func(j *Job) {
					time.Sleep(20 * time.Millisecond)
					select {
					case d.IncomingJob <- j:
					default:
						logger.Printf("[Dispatcher] Queue full, dropping job %s\n", j.ID)
					}
				}(job)
			}

		case <-d.stopChan:
			logger.Println("[Dispatcher] Graceful shutdown initiated")
			close(d.IncomingJob)
			for _, w := range d.Workers {
				w.Stop()
			}
			logger.Println("[Dispatcher] Graceful shutdown completed")
			return

		case <-d.quitChan:
			logger.Println("[Dispatcher] Force shutdown initiated")
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

func (d *Dispatcher) getNextWorker() *Worker {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.Workers) == 0 {
		return nil
	}
	w := d.Workers[d.rrIndex]
	d.rrIndex = (d.rrIndex + 1) % len(d.Workers)
	return w
}

func (d *Dispatcher) handleResult(res Result) {
	if res.Status == JobSuccess {
		d.WorkerMetrics.IncSuccessful()
		return
	}

	job := res.Job
	job.Retries++
	d.WorkerMetrics.IncJobRetry()

	if job.Retries <= d.MaxRetries {
		d.WorkerMetrics.IncJobRetry()

		delay := time.Duration(1<<uint(job.Retries-1)) * time.Second
		logger.Printf("🔁 Retrying job %s (%d/%d) in %v\n", job.ID, job.Retries, d.MaxRetries, delay)

		go func(j *Job, delay time.Duration) {
			time.Sleep(delay)
			select {
			case d.IncomingJob <- j:
			default:
				logger.Printf("⚠️ Retry queue full, moving job %s to DLQ\n", j.ID)
				d.mu.Lock()
				d.DLQ = append(d.DLQ, j)
				d.mu.Unlock()
			}
		}(job, delay)

		return
	}

	d.WorkerMetrics.IncFailed()
	logger.Printf("☠️ Job %v moved to DLQ after %d retries lmaooooooo\n", job.Name, job.Retries-1)
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
			w.Stop()
			d.Workers = append(d.Workers[:i], d.Workers[i+1:]...)
			logger.Printf("[Autoscaler] scaled down → %d workers\n", len(d.Workers))
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

	logger.Printf("[Autoscaler] scaled up → %d workers\n", len(d.Workers))
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

func (d *Dispatcher) StartMetricsLogger(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.WorkerMetrics.LogMetrics()
			case <-d.stopChan:
				logger.Println("[MetricsLogger] Graceful shutdown")
				return
			case <-d.quitChan:
				logger.Println("[MetricsLogger] Force shutdown")
				return
			}
		}
	}()
}

func (d *Dispatcher) Close() {
	d.stopChan <- true
	d.wg.Wait()
}

func (d *Dispatcher) Kill() {
	close(d.quitChan)
}
