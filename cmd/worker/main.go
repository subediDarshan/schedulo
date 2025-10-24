package main

import (
	"flag"

	"github.com/subediDarshan/schedulo/pkg/worker"
)

var (
	serverPort      = flag.String("worker_port", "", "Port on which the Worker serves requests.")
	coordinator = flag.String("coordinator", "coordinator:8080", "Network address of the Coordinator.")
)

func main() {
	flag.Parse()

	worker := worker.NewServer(*serverPort, *coordinator)
	worker.Start()
}
