// Package server for debug, name it 'main' so `go run server.go` is valid
// Package server for release, name it 'server' for better maintenance
package server

import (
	"../lib"
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

func TcpListen(port string, fileNode *lib.FilesysNode) {
	service := new(lib.Service)
	service.FileNode = fileNode
	rpc.Register(service)
	rpc.Register(lib.Sw)
	rpc.Register(lib.Sm)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("lib.Service successfully registered at port", port)
	go log.Fatal(http.Serve(l, nil))
}

func LogSetUp() {
	// create log file
	// var filename string = "Logs/" + "Node" + newNode.Name + newNode.Port + ".log"
	filename := "Logs/" + "Machine.log"
	logFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Printf("File open error=%s\r\n", err.Error())
		os.Exit(-1)
	}
	multiw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiw)
	log.Println("Start logging...")
}

func PutCommandSetUp(cmd []string, newFilesysNode *lib.FilesysNode) {
	if len(cmd) != 3 {
		log.Printf("The length of put command should be 3.\n")
	} else {
		targetPath := lib.FileSysLocation + cmd[2]
		log.Println("put file [" + cmd[1] + "] to [" + targetPath + "]\n")
		if putTime, ok := newFilesysNode.FilePutTime[targetPath]; ok {
			timeCommand := time.Now()
			if timeCommand.Sub(putTime).Seconds() > 60 {
				PutCommandExecute(cmd, newFilesysNode)
			} else {
				log.Printf("write to the same file within 1 min. Do you want to continue?\n")
				log.Printf("Please reply [yes] or [no].\n")
				reader := bufio.NewReader(os.Stdin)
				text, _ := reader.ReadString('\n')
				if text == "yes\n" && time.Now().Sub(timeCommand).Seconds() < 30 {
					tSum := time.Now()
					PutCommandExecute(cmd, newFilesysNode)
					timeCostsum := time.Since(tSum)
					log.Println("Put file time cost: ", timeCostsum)
				} else {
					log.Printf("Put rejected.\n")
				}
			}
		} else {
			tSum := time.Now()
			PutCommandExecute(cmd, newFilesysNode)
			timeCostsum := time.Since(tSum)
			log.Println("Put file time cost: ", timeCostsum)
		}
	}
}

func PutListCommandSetUp(cmd []string, newFilesysNode *lib.FilesysNode) {
	if len(cmd) != 3 {
		log.Printf("The length of put command should be 3.\n")
	} else {
		directory := cmd[1]
		filename := cmd[2]
		files, err := ioutil.ReadDir(directory)
		if err != nil {
			log.Println("PutListCommandSetUp readdir error")
		}
		for _, f := range files {
			if strings.Contains(f.Name(), filename) {
				tSum := time.Now()
				putcmd := []string{"put", cmd[1] + f.Name(), f.Name()}
				PutCommandExecute(putcmd, newFilesysNode)
				timeCostsum := time.Since(tSum)
				log.Println("Put file time cost: ", timeCostsum)
			}
		}
	}
}

func PutCommandExecute(cmd []string, newFilesysNode *lib.FilesysNode) {
	targetPath := lib.FileSysLocation + cmd[2]
	data, err := ioutil.ReadFile(cmd[1])
	if err != nil {
		log.Printf("Put error: local filename not exists\n")
	} else {
		file := lib.FileIn{
			FileName: targetPath,
			Data:     data,
			Append:   false,
		}
		newFilesysNode.PutFileRemoteCall(file)
		// broadcase the put information to all other nodes
		newFilesysNode.PutInfoBroadcast(file.FileName, time.Now())
	}
}

func GetCommandSetUp(cmd []string, newFilesysNode *lib.FilesysNode) {
	if len(cmd) != 3 {
		log.Printf("The length of get command should be 3.\n")
	} else {
		targetPath := lib.FileSysLocation + cmd[1]
		log.Println("get file [" + targetPath + "] to [" + cmd[2] + "]\n")
		data, err := newFilesysNode.GetFileRemoteCall(targetPath)
		if err != nil {
			log.Printf("Get error: remote get procedure failed\n")
		} else {
			tSum := time.Now()
			lib.PutFile(cmd[2], data)
			timeCostsum := time.Since(tSum)
			log.Println("Get file time cost: ", timeCostsum)
		}
	}
}

func DeleteCommandSetUp(cmd []string, newFilesysNode *lib.FilesysNode) {
	if len(cmd) != 2 {
		log.Printf("The length of delete command should be 2.\n")
	} else {
		targetPath := lib.FileSysLocation + cmd[1]
		log.Println("delete file [" + targetPath + "]\n")
		tSum := time.Now()
		newFilesysNode.DeleteFileRemoteCall(targetPath)
		timeCostsum := time.Since(tSum)
		log.Println("Delete file time cost: ", timeCostsum)

	}
}

func StoreCommandSetUp(cmd []string, newFilesysNode *lib.FilesysNode) {
	log.Println("List all the files stored on this machine:" + "\n")
	if len(cmd) != 1 {
		log.Printf("The length of store command should be 1.\n")
	} else {
		newFilesysNode.Store()
	}
}

func ListCommandSetUp(cmd []string, newFilesysNode *lib.FilesysNode) {
	if len(cmd) != 2 {
		log.Printf("The length of list command should be 1.\n")
	} else {
		targetPath := lib.FileSysLocation + cmd[1]
		log.Println("List all the nodes store on target file \n")
		err := newFilesysNode.ListFileRemoteCall(targetPath)
		if err != nil {
			log.Printf("Listing files error.\n")
		}
	}
}

func MapleSetUp(cmd []string) {
	if len(cmd) != 5 {
		log.Printf("The length of list command should be 4.\n")
	} else {
		mapleExe := cmd[1]
		numMaples := cmd[2]
		sdfsIntermediateFilenamePrefix := lib.Local + cmd[3]
		sdfsSrcDirectory := lib.FileSysLocation + cmd[4]
		log.Println("Run maple command \n")
		IntNumMaple, _ := strconv.Atoi(numMaples)
		go lib.Mr.DoMaple(IntNumMaple, sdfsIntermediateFilenamePrefix, sdfsSrcDirectory, mapleExe)
	}
}

func JuiceSetUp(cmd []string) {
	if len(cmd) != 6 {
		log.Printf("The length of list command should be 5.\n")
	} else {
		juiceExe := cmd[1]
		numJuices := cmd[2]
		sdfsIntermediateFilenamePrefix := lib.FileSysLocation + cmd[3]
		sdfsDestFilename := lib.FileSysLocation + cmd[4]
		deleteInput := cmd[5]
		log.Println("Run juice command \n")
		IntNumJuices, _ := strconv.Atoi(numJuices)
		IntDeleteInput, _ := strconv.Atoi(deleteInput)
		go lib.Mr.DoJuice(IntNumJuices, sdfsIntermediateFilenamePrefix, sdfsDestFilename, juiceExe, IntDeleteInput)
	}
}

func GetMyIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				log.Println("My ip address is: ", ipnet.IP.String())
				return ipnet.IP.String()
			}
		}
	}
	return ""

}
