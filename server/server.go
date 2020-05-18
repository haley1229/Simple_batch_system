// Package server for debug, name it 'main' so `go run server.go` is valid
// Package server for release, name it 'server' for better maintenance
package server

import (
	// built-in packages
	"bufio"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"../lib"
)

// Launch function adds new node
func Launch(port string, mode string) {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal("Can not get host name from the node")
		os.Exit(1)
	}
	log.Printf("Starting %s, \n", name)

	LogSetUp()
	GetMyIP()

	newNode := new(lib.Node)
	newNode.Name = mode
	newNode.Port = port
	newNode.IP = lib.GetIP(name)
	// newNode.IP = lib.Introducerip //lib.GetIP(name) //TODO: change here

	newFilesysNode := new(lib.FilesysNode)
	newFilesysNode.MemberNode = newNode
	newFilesysNode.FilePutTime = make(map[string]time.Time)
	newFilesysNode.FileLocations = make(map[string][]int)

	lib.Wk.MemberNode = newNode
	lib.Wk.FileNode = newFilesysNode

	lib.Mr.MemberNode = newNode
	lib.Mr.FileNode = newFilesysNode

	go TcpListen(port, newFilesysNode)
	go SystemListen(newNode, newFilesysNode)
	for {
		time.Sleep(1000000000)
	}
	log.Println("The node ", name, " has exited! ")
}

func SystemListen(newNode *lib.Node, newFilesysNode *lib.FilesysNode) {
	newNode.Status = lib.Failed
	go newNode.ListenToUDP()
	go func() {
		log.Println("Options: [leave] for leave the membership, [join] for join the membership, [print] for print the list and neighbors, [ID] for print ID")
		log.Println("Options: [put] for put file to the filesystem")
		for {
			// membership listen
			reader := bufio.NewReader(os.Stdin)
			text, _ := reader.ReadString('\n')
			if text == "clear\n" {
				cmd := exec.Command("clear")
				cmd.Stdout = os.Stdout
				cmd.Run()
			} else if text == "ID\n" {
				newNode.PrintID()
			} else if text == "print\n" {
				newNode.PrintMembership()
				newNode.PrintNeighbor()
			} else if text == "join\n" {
				if newNode.Status == lib.Alive {
					log.Println("This node is already in the group")
				} else {
					log.Println("Join the group")
					go newNode.JoinNewNode()
					go newNode.KeepAlive()
					// heartbeat of the filesysnode
					go newFilesysNode.HeartBeat()
					// clear the filesys
					os.RemoveAll(lib.FileSysLocation)
					os.Mkdir(lib.FileSysLocation, 0777)
					// clear the local files
					os.RemoveAll(lib.Local)
					os.Mkdir(lib.Local, 0777)
				}
			} else if text == "leave\n" {
				if newNode.Status == lib.Alive {
					log.Println("Leave the group")
					newNode.LeaveGroup()
				} else {
					log.Println("Node is not in the group")
				}
			}
			// file system listen
			cmd := strings.Split(strings.TrimSuffix(text, "\n"), " ")
			if cmd[0] == "put" {
				PutCommandSetUp(cmd, newFilesysNode)
			} else if cmd[0] == "putlist" {
				PutListCommandSetUp(cmd, newFilesysNode)
			} else if cmd[0] == "get" {
				GetCommandSetUp(cmd, newFilesysNode)
			} else if cmd[0] == "delete" {
				DeleteCommandSetUp(cmd, newFilesysNode)
			} else if cmd[0] == "ls" {
				ListCommandSetUp(cmd, newFilesysNode)
			} else if cmd[0] == "store" {
				StoreCommandSetUp(cmd, newFilesysNode)
			} else if cmd[0] == "printfile" {
				newFilesysNode.PrintFile()
			}
			// mapreduce listen
			if cmd[0] == "maple" {
				MapleSetUp(cmd)
			} else if cmd[0] == "juice" {
				JuiceSetUp(cmd)
			} else if cmd[0] == "printqueue" {
				lib.Wk.Printqueue()
			}
		}
	}()
	log.Printf("Log file saved!\n")
}
