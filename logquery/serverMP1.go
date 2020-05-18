package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os/exec"
)

// GrepOperation is the exported type, for remotely function call
type GrepOperation int

// Grep reads the expression from command line and check whether there are matches in the log
func (g *GrepOperation) Grep(grepPattern []string, result *string) error {

	filename := "Logs/" + "Machine.log"
	//fmt.Println(fname)

	cmd := exec.Command("grep", grepPattern[0], grepPattern[1], filename)
	resultsBytes, err := cmd.CombinedOutput()

	if err != nil {
		fmt.Println(err.Error())
		return err
	} else {
		//fmt.Printf("%s\n", resultsBytes)
		*result = filename + "\n" + string(resultsBytes)
		return nil
	}
}

func main() {
	flag.Parse()
	var portNum = flag.String("port", "5555", "Input the port number")
	rpc.RegisterName("GrepOperation", new(GrepOperation))

	port := ":" + *portNum

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		rpc.ServeConn(conn)
	}
}
