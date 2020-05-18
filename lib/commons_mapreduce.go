package lib

import (
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// PutFile -- put file to target node
func PutFile(filename string, data []byte) {
	localfile, error := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if error != nil {
		log.Fatal("File open error:", error)
	}
	_, writeError := localfile.Write(data)
	if writeError != nil {
		log.Fatal("File put error:", writeError)
	}
	localfile.Close()
}

func (Wk *Worker) updateQueues(inFile string) {
	Wk.finishedFileQueue = append(Wk.finishedFileQueue, inFile)
	if len(Wk.fileQueue) == 1 {
		Wk.fileQueue = []string{}
		return
	}
	targetIndex := 0
	for index, filename := range Wk.fileQueue {
		if filename == inFile {
			targetIndex = index
			break
		}
	}
	Wk.fileQueue[targetIndex] = Wk.fileQueue[len(Wk.fileQueue)-1]
	Wk.fileQueue = Wk.fileQueue[:len(Wk.fileQueue)-1]
}

func (Mr *Master) isLocalWorker(worker TargetNode) bool {
	memberNode := Mr.FileNode.MemberNode
	if worker.IP == memberNode.IP && worker.Port == memberNode.Port {
		return true
	}
	return false
}

func (Wk *Worker) isLocalMaster() bool {
	memberNode := Wk.FileNode.MemberNode
	if Wk.MasterIP == memberNode.IP && Wk.MasterPort == memberNode.Port {
		return true
	}
	return false
}

func TCPDialWrapper(IP string, Port string, name string, input interface{}, output bool) {
	// var wg sync.WaitGroup
	// wg.Add(1)
	// go func() {
	client, err := rpc.DialHTTP("tcp", IP+":"+Port)
	if err != nil {
		log.Printf(name, " dialing error:", err)
	}
	err = client.Call(name, &input, &output)
	if err != nil {
		log.Printf(name, " call error:", err)
	}
	client.Close()
	// }()
	// go func() {
	// 	wg.Wait()
	// }()
}

func (Mr *Master) FileTransferRequest(filename string, id int) {
	worker := getWorkerFromId(id)
	if Mr.isLocalWorker(worker) {
		Wk.GetFile(filename)
		return
	}
	if Mr.FileNode.checkStatus(id) != -1 {
		go func() {
			response := false
			TCPDialWrapper(worker.IP, worker.Port, "ServiceWorker.GetFile", filename, response)
		}()
	}
}

func (Mr *Master) LeastLoadID() int {
	targetID := 0
	targetLoad := 10000000
	for id, load := range Mr.WorkerLoad {
		if load < targetLoad {
			targetID = id
			targetLoad = load
		}
	}
	return targetID
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func getWorkerFromId(id int) TargetNode {
	//TODO
	hostname := getHostName(id)
	IP := GetIP(hostname)
	port := strconv.Itoa(1234)
	// IP := Introducerip
	// port := strconv.Itoa(1234 + id)
	return TargetNode{
		IP:   IP,
		Port: port,
		ID:   id,
	}
}
