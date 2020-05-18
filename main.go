package main

import (
	"./server"
	"flag" // command-line flag parsing
	"fmt"
	"log"
	"math/rand"
	"time"
)

var mode = flag.String("m", "", "-m introducer OR -m server to run introducer/server code")

// var vm = flag.String("vm", "", "[-m client ONLY] file path that records IP and Path info of VMs")
// var command = flag.String("c", "", "[-m client ONLY] command args to be executed, e.g. -c \"grep [options]\" (Note: must be passed-in as string!)")
// var port = flag.String("p", "1234", "port to listen")
var port = flag.String("port", "5555", "port to listen")

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	flag.Parse() // parse flags and store in pointer

	if *mode == "introducer" || *mode == "server" { // Note: deference * to access flag!
		fmt.Printf("Launching %s...\n", *mode)
		server.Launch(*port, *mode)
	} else {
		log.Fatal("missing flags...main -help for usage")
	}

}
