package lib

import (
	"io/ioutil"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"time"
	// "encoding/gob"
)

var FileSysLocation = "/home/lma16/filesys/"
var Local = "/home/lma16/local/"
var introducerid = 4
// var FileSysLocation = "/Users/linjian/Documents/cs425-fa19/mp4/filesystem/" //TODO
// var Local = "/Users/linjian/Documents/cs425-fa19/mp4/local/"

type Master struct {
	Timer         sync.Mutex
	MapleExe      string
	JuiceExe      string
	FileNode      *FilesysNode
	MemberNode    *Node
	Workers       []TargetNode
	WorkerLoad    map[int]int    // how many files are assigned to each worker
	FileCondition map[string]int // 0 unsent, 1 sent to worker, 2 finished
	FileLocation  map[string]int // FIXME: the id that map/reduce will be performed on
	LocationFiles map[int][]string
	AllFilenames  []string
	duration      time.Duration
}

type InitIn struct {
	Exe            string
	ExeFile        []byte
	FilenamePrefix string
	MasterIP       string
	MasterPort     string
}

type ServiceMaster struct {
	Mr *Master
}

var Mr = new(Master)
var Sm = new(ServiceMaster)

// received the notification that the map/reduce of filename is finished.
func (Mr *Master) ReceiveFinish(filenames []string) {
	log.Println("outside of lock")
	Mr.Timer.Lock()
	log.Println("in lock")
	for _, filename := range filenames {
		Mr.FileCondition[filename] = 2
	}
	Mr.Timer.Unlock()
}

// received the notification that the map/reduce of filename is finished from rpc.
func (Sm *ServiceMaster) ReceiveFinish(filenames interface{}, response *bool) error {
	fnames, _ := filenames.([]string)
	Sm.Mr.ReceiveFinish(fnames)
	*response = true
	return nil
}

func (Mr *Master) DoMaple(
	numMaples int,
	sdfsIntermediateFilenamePrefix string,
	sdfsSrcDirectory string,
	MapleExe string) {
	start := time.Now()
	// set up master
	Sm.Mr = Mr
	Mr.MapleExe = MapleExe
	Mr.SetUpWorkers(numMaples, sdfsIntermediateFilenamePrefix, "map")
	Mr.SetUpFile(sdfsSrcDirectory)
	Mr.DistribiteFiles()
	Mr.broadcastFinish(0)
	log.Println("map phase finised on master")
    Mr.duration = time.Since(start)
	log.Println("map took", Mr.duration, "seconds")
}

func (Mr *Master) DoJuice(
	numJuices int,
	sdfsIntermediateFilenamePrefix string,
	sdfsDestFilename string,
	juiceExe string,
	deleteInput int) {
	start := time.Now()
	// set up master
	Sm.Mr = Mr
	Mr.JuiceExe = juiceExe
	Mr.SetUpWorkers(numJuices, sdfsDestFilename, "reduce")
	Mr.SetUpFile(sdfsIntermediateFilenamePrefix)
	Mr.DistribiteFiles()
	Mr.broadcastFinish(deleteInput)
	if deleteInput == 1 {
		//FIXME: delete the inputs
		log.Println("delete intermediate files")
		for filename, _ := range Mr.FileCondition {
			Mr.FileNode.DeleteFileRemoteCall(filename)
		}
	}
	log.Println("reduce phase finised on master")
    Mr.duration += time.Since(start)
	log.Println("mapreduce took", Mr.duration, "seconds")
}

func (Mr *Master) DistribiteFiles() {
	PhaseComplete := false
	// FIXME: check here to update the workers list
	for PhaseComplete == false {
		PhaseComplete = true
		// update workers
		Mr.Timer.Lock()
		workers := []TargetNode{}
		for _, worker := range Mr.Workers {
			if Mr.FileNode.checkStatus(worker.ID) != -1 {
				workers = append(workers, worker)
			}
		}
		Mr.Workers = workers
		Mr.Timer.Unlock()
		// finish update workers
		FileConditionCopy := map[string]int{}
		Mr.Timer.Lock()
		for filename, condition := range Mr.FileCondition {
			FileConditionCopy[filename] = condition
		}
		Mr.Timer.Unlock()
		// here use fileconditioncopy to avoid lock of filecondiiton
		for filename, _ := range FileConditionCopy {
			Mr.Timer.Lock()
			condition := Mr.FileCondition[filename]
			Mr.Timer.Unlock()
			if condition == 0 {
				PhaseComplete = false
				// check the mapfile locations, see if there're workers with load < 2
				Mr.DistributeOneFile(filename)
			} else if condition == 1 {
				// FIXME: if condition is 1, check the place it is sent to, if the node is not in the worker, redistribute it.
				if !Mr.isworker(Mr.FileLocation[filename]) {
					Mr.DistributeOneFile(filename)
				}
				PhaseComplete = false
			}
		}
		Mr.notifyFiles()
	}
}

func (Mr *Master) DistributeOneFile(filename string) {
	Complete := false
	Mr.FileNode.timer.Lock()
	for _, id := range Mr.FileNode.FileLocations[filename] {
		// FIXME: check the id is in the workers
		if Mr.WorkerLoad[id] < 2 && Mr.isworker(id) {
			// tell the worker to put the file on the queue
			Mr.PutFileOnQueue(filename, id)
			Complete = true
			break
		}
	}
	Mr.FileNode.timer.Unlock()
	// send the file to the worker with least load
	if !Complete {
		// get the least load worker
		id := Mr.LeastLoadID()
		Mr.FileNode.timer.Lock()
		if contains(Mr.FileNode.FileLocations[filename], id) {
			// tell the worker to put the file on the queue
			Mr.PutFileOnQueue(filename, id)
		} else {
			// send the file, and tell the worker to put the file on the queue
			Mr.FileTransferRequest(filename, id)
			Mr.PutFileOnQueue(filename, id)
		}
		Mr.FileNode.timer.Unlock()
	}
}

func (Mr *Master) PutFileOnQueue(filename string, id int) {
	// worker := getWorkerFromId(id)
	// if Mr.isLocalWorker(worker) {
	// Wk.PutFileOnQueue(filename)
	Mr.Timer.Lock()
	Mr.WorkerLoad[id]++
	if id == introducerid {
		Mr.WorkerLoad[id] = Mr.WorkerLoad[id] + 2
	}
	Mr.FileCondition[filename] = 1
	Mr.FileLocation[filename] = id
	if _, ok := Mr.LocationFiles[id]; ok {
		Mr.LocationFiles[id] = append(Mr.LocationFiles[id], filename)
	} else {
		Mr.LocationFiles[id] = []string{filename}
	}
	Mr.Timer.Unlock()
	return
	// }
	// if Mr.FileNode.checkStatus(id) != -1 {
	// 	// go func() {
	// 	// for i := 1;  i<=100; i++ {
	// 	response := false
	// 	TCPDialWrapper(worker.IP, worker.Port, "ServiceWorker.PutFileOnQueue", filename, response)
	// 	// }
	// 	// }()
	// 	Mr.Timer.Lock()
	// 	Mr.WorkerLoad[id]++
	// 	Mr.FileCondition[filename] = 1
	// 	Mr.Timer.Unlock()
	// }
}

func (Mr *Master) notifyFiles() {
	Mr.Timer.Lock()

	LocationFilesCopy := map[int][]string{}
	for id, filenames := range Mr.LocationFiles {
		LocationFilesCopy[id] = filenames
	}
	for id, filenames := range LocationFilesCopy {
		// log.Println("send ", filenames, "to", id)
		worker := getWorkerFromId(id)
		if Mr.isLocalWorker(worker) {
			Wk.PutFilesOnQueue(filenames)
		} else {
			if Mr.FileNode.checkStatus(id) != -1 {
				// go func() {
				// for i := 1;  i<=100; i++ {
				response := false
				TCPDialWrapper(worker.IP, worker.Port, "ServiceWorker.PutFilesOnQueue", filenames, response)
				// }
				// }()
			}
		}
		delete(Mr.LocationFiles, id)
	}
	Mr.Timer.Unlock()
}

func (Mr *Master) SetUpFile(sdfsSrcDirectory string) {
	Mr.FileCondition = map[string]int{}
	Mr.FileLocation = map[string]int{}
	Mr.LocationFiles = map[int][]string{}
	// allFileLocations := Mr.FileNode.FileLocations
	Mr.AllFilenames = []string{}
	allFilenamesMap := Mr.GetAllFilenameDFS()
	log.Println(allFilenamesMap)
	for k, _ := range allFilenamesMap {
		Mr.AllFilenames = append(Mr.AllFilenames, k)
	}
	// Mr.FileNode.timer.Lock()
	// for k, _ := range Mr.FileNode.FileLocations {
	// 	allfilenames = append(allfilenames, k)
	// }
	log.Println(Mr.AllFilenames)
	log.Println(sdfsSrcDirectory)
	// Mr.FileNode.timer.Unlock()
	for _, filename := range Mr.AllFilenames {
		if strings.Contains(filename, sdfsSrcDirectory) {
			log.Println(filename)
			Mr.FileCondition[filename] = 0
		}
	}
}

func (Mr *Master) GetAllFilenameDFS() map[string]bool {
	allFilenamesMap := map[string]bool{}
	memberNode := Mr.FileNode.MemberNode
	activeNodes := Mr.FileNode.GetActiveNodes()
	for _, node := range activeNodes {
		if node.IP == memberNode.IP && node.Port == memberNode.Port {
			Mr.FileNode.timer.Lock()
			for k, _ := range Mr.FileNode.FileLocations {
				allFilenamesMap[k] = true
			}
			Mr.FileNode.timer.Unlock()
		} else if Mr.FileNode.checkStatus(node.ID) != -1 {
			response := []string{}
			input := true
			client, err := rpc.DialHTTP("tcp", node.IP+":"+node.Port)
			if err != nil {
				log.Println("get all filename dfs dialing error:", err)
			}
			err = client.Call("ServiceWorker.GetFilenames", &input, &response)
			if err != nil {
				log.Println("get all filename dfs call error:", err)
			}
			client.Close()
			for _, k := range response {
				allFilenamesMap[k] = true
			}
		}
	}
	return allFilenamesMap
}

func (Mr *Master) broadcastFinish(deleteInput int) {
	memberNode := Mr.FileNode.MemberNode
	for _, worker := range Mr.Workers {
		if worker.IP == memberNode.IP && worker.Port == memberNode.Port {
			time.Sleep(1000000000)
			Wk.FinishPhase(deleteInput)
		} else if Mr.FileNode.checkStatus(worker.ID) != -1 {
			response := false
			time.Sleep(1000000000)
			TCPDialWrapper(worker.IP, worker.Port, "ServiceWorker.FinishPhase", deleteInput, response)
		}
	}
}

func (Mr *Master) isworker(id int) bool {
	for _, worker := range Mr.Workers {
		if worker.ID == id {
			return true
		}
	}
	return false
}

func (Mr *Master) SetUpWorkers(
	numWorkers int,
	FilenamePrefix string,
	phase string) {
	// map and reduce setups
	exe := Mr.MapleExe
	exefile, _ := ioutil.ReadFile(Mr.MapleExe)
	rpcName := "ServiceWorker.MapInit"
	InitFunc := Wk.MapInit
	if phase == "reduce" {
		exe = Mr.JuiceExe
		exefile, _ = ioutil.ReadFile(Mr.JuiceExe)
		rpcName = "ServiceWorker.ReduceInit"
		InitFunc = Wk.ReduceInit
	}

	// setup workers
	activeNodes := Mr.FileNode.GetActiveNodes()
	if numWorkers > len(activeNodes) {
		numWorkers = len(activeNodes)
	}
	Mr.Workers = activeNodes[:numWorkers]
	Mr.WorkerLoad = map[int]int{}
	for _, worker := range Mr.Workers {
		Mr.Timer.Lock()
		Mr.WorkerLoad[worker.ID] = 0
		Mr.Timer.Unlock()
	}
	// rpc to all the workers to notify Exe
	memberNode := Mr.FileNode.MemberNode
	initInfo := InitIn{
		Exe:            exe,
		ExeFile:        exefile,
		FilenamePrefix: FilenamePrefix,
		MasterIP:       memberNode.IP,
		MasterPort:     memberNode.Port,
	}
	for _, worker := range Mr.Workers {
		if worker.IP == memberNode.IP && worker.Port == memberNode.Port {
			InitFunc(initInfo)
		} else if Mr.FileNode.checkStatus(worker.ID) != -1 {

			client, err := rpc.DialHTTP("tcp", worker.IP+":"+worker.Port)
			if err != nil {
				log.Println("setupworker dialing error:", err)
			}
			response := false
			err = client.Call(rpcName, &initInfo, &response)
			if err != nil {
				log.Println("setupworker call error:", err)
			}
			client.Close()
		}
	}
}
