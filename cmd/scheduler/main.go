package main

import (
	"flag"
	"log"

	"github.com/subediDarshan/schedulo/pkg/common"
	"github.com/subediDarshan/schedulo/pkg/scheduler"
)

var schedulerPort = flag.String("scheduler_port", ":8081", "Port on which sheduler service runs on")

func main() {

	flag.Parse()

	dbConnectionString := common.GetDBConnectionString()

	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString)

	err := schedulerServer.Start()

	if err != nil {
		log.Fatalf("Error starting server. %+v", err)
	}

}