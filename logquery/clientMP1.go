package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//grepQuery use synchonized query to grep all vms
func grepQuery(hostip string, grepPattern []string, rcvchan chan []string) {
	go func() {
		results := make([]string, 1)
		results[0] = "From " + hostip + ": "

		//start timing
		t1 := time.Now()
		//try to connect with server
		//if failed, just return this goroutine and it will not affect others
		client, err := rpc.Dial("tcp", hostip)
		if err != nil {
			rcvchan <- append(results, "cannot connect to server "+hostip)
			return
		}
		var reply string
		//rpc -> call server function remotely
		err = client.Call("GrepOperation.Grep", grepPattern, &reply)
		if err != nil {
			//log.Fatal("calling error: ", err)
			rcvchan <- append(results, "Calling error from: "+hostip+" There may not be exsting matched pattern")
			return
		}

		scanner := bufio.NewScanner(strings.NewReader(reply))
		for scanner.Scan() {
			results = append(results, scanner.Text()+"\n") // Println will add back the final '\n'
		}

		timeCost := time.Since(t1)
		//fmt.Println("Query cost: ", timeCost)
		times := fmt.Sprintln(hostip, " costs ", timeCost)
		results = append(results, times)

		rcvchan <- results

	}()

}

var portNum = flag.String("port", "5555", "Input the port number")
var grepOptions = flag.String("option", "-n", "Input grep options")
var userGrepPattern = flag.String("pattern", "unique", "Input grep patterns")
var totalVms = flag.Int("vm", 10, "Input the number of servers you plan to query")

func main() {
	flag.Parse()
	if *totalVms > 10 {
		fmt.Println("No more than 10 VMs!")
		os.Exit(1)
	}
	nodeList := [10]string{"fa19-cs425-g62-01.cs.illinois.edu", "fa19-cs425-g62-02.cs.illinois.edu", "fa19-cs425-g62-03.cs.illinois.edu", "fa19-cs425-g62-04.cs.illinois.edu", "fa19-cs425-g62-05.cs.illinois.edu", "fa19-cs425-g62-06.cs.illinois.edu", "fa19-cs425-g62-07.cs.illinois.edu", "fa19-cs425-g62-08.cs.illinois.edu", "fa19-cs425-g62-09.cs.illinois.edu", "fa19-cs425-g62-10.cs.illinois.edu"}

	port := ":" + *portNum
	clientChann := make(chan []string)
	hostName, err := os.Hostname()
	if err != nil {
		log.Fatal("hostname error: ", err)
	}

	tsum := time.Now()
	//query each vm to get response in channel
	for machine := 0; machine < *totalVms; machine++ {
		var mypattern []string
		mypattern = append(mypattern, *grepOptions)
		mypattern = append(mypattern, *userGrepPattern)
		if nodeList[machine] == hostName {
			fmt.Println("skip the local: ", hostName)
			continue
		}
		grepQuery(nodeList[machine]+port, mypattern, clientChann)
	}

	//print results and time used
	var timediff []string
	var lines [10]int

	// Things may happen when you use VM n to grep VM 0-m (n>m), you will lose one responding message
	//TODO: How channel works and how to know whether it's empty or read to read
	for readnum := 0; readnum < *totalVms-1; readnum++ {
		finalResult := <-clientChann

		if len(finalResult) != 0 {
			for i := 2; i < len(finalResult)-1; i++ {
				finalResult[i] = finalResult[0] + strings.Replace(finalResult[1], "\n", " ", -1) + finalResult[i]
				fmt.Println(finalResult[i])
			}
			//fmt.Println(finalResult[2:len(finalResult)-1])
			timencount := fmt.Sprintln(finalResult[len(finalResult)-1], "Lines found: ", len(finalResult)-3)
			lines[readnum] = len(finalResult) - 3
			timediff = append(timediff, timencount)
		}

	}
	// Time used for all queries
	timeCostsum := time.Since(tsum)
	sum := 0
	for p := 0; p < len(timediff); p++ {
		fmt.Println(timediff[p])
		if lines[p] > -1 {
			sum += lines[p]
		}
	}
	fmt.Println("total lines found: ", sum)
	fmt.Println("Multiple Query cost: ", timeCostsum)

}
