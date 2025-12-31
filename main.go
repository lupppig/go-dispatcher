package main

import (
	"fmt"
)

func main() {
	dispatcher := NewDispatcher(4)
	dispatcher.Run()

	for i := 0; i < 20; i++ {
		job := NewJob(
			fmt.Sprintf("payload-%d", i),
			fmt.Sprintf("send %d", i),
			3,
		)

		dispatcher.IncomingJob <- job
	}

	// time.Sleep(5 * time.Second)

	// dispatcher.Close() // graceful shutdown

	select {}
}
