package main

import (
	"fmt"
	"log"
	"time"
)

var logger *log.Logger

func main() {
	dispatcher := NewDispatcher(10)
	dispatcher.Run()

	fmt.Println("-------------------------> message dispatcher started <--------------------------")
	logger = initLogger()
	for i := 0; i < 1000; i++ {
		jobName := fmt.Sprintf("send %d", i)
		if i%2 == 0 {
			jobName = "no job name"
		}
		job := NewJob(
			fmt.Sprintf("payload-%d", i),
			jobName,
			3,
		)

		dispatcher.IncomingJob <- job
	}

	time.Sleep(1 * time.Second)
	dispatcher.Close()
	fmt.Println("-----------------------------------> message dispatcher closed <-------------------------")
}
