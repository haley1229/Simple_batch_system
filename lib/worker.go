package lib

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	// "net/rpc"
	"fmt"
	// "encoding/gob"
	"bufio"
	"encoding/json"
	"plugin"
	// "strconv"
	"time"
	"unicode/utf8"
	// "runtime/debug"
)

type Worker struct {
	Timer             sync.Mutex
	MasterIP          string
	MasterPort        string
	Mapfunc           func(string, string) [][2]string
	Reducefunc        func(string, []string) string
	FileNode          *FilesysNode
	MemberNode        *Node
	FilenamePrefix    string
	fileQueue         []string
	finishedFileQueue []string
	MapKeysFiles      map[string]*os.File
	MapKeysEncoders   map[string]*json.Encoder
	ReduceFile        *os.File
	ReduceEncoder     *json.Encoder
	phase             int // 0 stop, 1 map, 2 reduce
}

type ServiceWorker struct {
	Wk *Worker
}

var Wk = new(Worker)
var Sw = new(ServiceWorker)

func (Sw *ServiceWorker) MapInit(initInfo *InitIn, response *bool) error {
	Sw.Wk = Wk
	Sw.Wk.MapInit(*initInfo)
	*response = true
	return nil
}

func (Sw *ServiceWorker) ReduceInit(initInfo *InitIn, response *bool) error {
	Sw.Wk = Wk
	Sw.Wk.ReduceInit(*initInfo)
	*response = true
	return nil
}

func (Wk *Worker) MapInit(initInfo InitIn) {
	log.Println("map phase inits.")
	Wk.MasterIP = initInfo.MasterIP
	Wk.MasterPort = initInfo.MasterPort
	Wk.Mapfunc = Wk.getMapFunc(initInfo)
	Wk.FilenamePrefix = initInfo.FilenamePrefix
	Wk.fileQueue = []string{}
	Wk.finishedFileQueue = []string{}
	Wk.phase = 1
	go Wk.MapSchedule()
}

func (Wk *Worker) ReduceInit(initInfo InitIn) {
	log.Println("reduce phase inits.")
	// debug.PrintStack()
	Wk.MasterIP = initInfo.MasterIP
	Wk.MasterPort = initInfo.MasterPort
	Wk.Reducefunc = Wk.getReduceFunc(initInfo)
	Wk.FilenamePrefix = initInfo.FilenamePrefix
	Wk.fileQueue = []string{}
	Wk.finishedFileQueue = []string{}
	Wk.phase = 2
	go Wk.ReduceSchedule()
}

func (Wk *Worker) MapSchedule() {
	for Wk.phase == 1 {
		time.Sleep(1000000)
		Wk.Timer.Lock()
		fileQueueCopy := Wk.fileQueue
		Wk.Timer.Unlock()
		if len(fileQueueCopy) > 0 {
			for _, inFile := range fileQueueCopy {
				for Exists(inFile) == false {
					time.Sleep(1000)
				}
				log.Println("Start performing map for", inFile)
				file, err := os.Open(inFile)
				if err != nil {
					log.Println(err)
				}
				scanner := bufio.NewScanner(file)
				buf := make([]byte, 0, 12)
				scanner.Buffer(buf, bufio.MaxScanTokenSize)
				text := ""
				iter := 0
				for scanner.Scan() {
					text = text + "\n" + scanner.Text()
					iter++
					if iter%1000 == 0 {
						Wk.doMap(inFile, text)
						text = ""
					}
				}
				if iter != 1000 {
					Wk.doMap(inFile, text)
				}
				file.Close()
				log.Println("map for", inFile, " finished.")
				// Wk.notifyFinish(inFile) // FIXME: this should be done at last
				Wk.Timer.Lock()
				Wk.updateQueues(inFile)
				Wk.Timer.Unlock()
				// }()
			}
		}
		Wk.notifyFinish()
	}
	// close all the encoders
	Wk.writeFilesToDFS()
	log.Printf("Map phase finished.")
}

func (Wk *Worker) ReduceSchedule() {
	fd, _ := os.OpenFile(Wk.FilenamePrefix, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	Wk.ReduceFile = fd
	Wk.ReduceEncoder = json.NewEncoder(fd)
	for Wk.phase == 2 {
		Wk.Timer.Lock()
		fileQueueCopy := Wk.fileQueue
		Wk.Timer.Unlock()
		if len(fileQueueCopy) > 0 {
			for _, inFile := range fileQueueCopy {
				for Exists(inFile) == false {
					time.Sleep(1000)
				}
				log.Println("Start performing reduce for", inFile)
				Wk.doReduce(inFile)
				log.Println("reduce for", inFile, " finished.")
				// Wk.notifyFinish(inFile) // FIXME: this should be done at last
				Wk.Timer.Lock()
				Wk.updateQueues(inFile)
				Wk.Timer.Unlock()
			}
		}
		Wk.notifyFinish()
	}
	fd.Close()
	log.Printf("Reduce phase finished.")
}

func (Wk *Worker) doReduce(inFile string) {
	for Wk.Reducefunc == nil {
		time.Sleep(1000)
	}
	fd, err := os.OpenFile(inFile, os.O_RDONLY, 0600)
	if err != nil {
		log.Println("open the reduce input file failed")
	}
	decoder := json.NewDecoder(fd)

	key := ""
	values := []string{}
	kv := [2]string{}
	for {
		err := decoder.Decode(&kv)
		if err != nil {
			break
		}
		key = kv[0]
		values = append(values, kv[1])
	}
	fd.Close()
	// Apply reduce f() and write results
	kvpair := [2]string{key, reduncefunc(key, values)}
	Wk.ReduceEncoder.Encode(kvpair)
}

// func mapfunc(file string, value string) [][2]string {
// 	words := strings.Fields(value)
// 	res := [][2]string{}
// 	for _, w := range words {
// 		kv := [2]string{w, ""}
// 		res = append(res, kv)
// 	}
// 	return res
// }

// func reduncefunc(key string, values []string) string {
// 	return strconv.Itoa(len(values))
// }

func mapfunc(file string, value string) [][2]string {
	lines := strings.Split(value, "\n")
	res := [][2]string{}
	for _, line := range lines {
		words := strings.Fields(line)
		if len(words) == 2{
			kv := [2]string{words[1], words[0]}
			res = append(res, kv)
		}
	}
	return res
}

func reduncefunc(key string, values []string) string {
	return strings.Join(values[:], ", ")
}

func (Wk *Worker) doMap(filename string, data string) { //data []byte) {
	Wk.MapKeysFiles = map[string]*os.File{}
	Wk.MapKeysEncoders = map[string]*json.Encoder{}
	for Wk.Mapfunc == nil {
		time.Sleep(1000)
	}
	kvpairs := mapfunc(filename, data) //Wk.Mapfunc(filename, data)//FIXME
	// Write all K-V pairs to output files
	for _, kvpair := range kvpairs {
		fixUtf := func(r rune) rune {
			if r == utf8.RuneError {
				return -1
			}
			return r
		}
		kvpair[0] = strings.Map(fixUtf, kvpair[0])
		kvpair[0] = strings.Replace(kvpair[0], "?", "", -1)
		kvpair[0] = strings.Replace(kvpair[0], "/", "", -1)
		encoder, ok := Wk.MapKeysEncoders[kvpair[0]]
		if ok {
			err := encoder.Encode(kvpair)
			if err != nil {
				log.Println("Wk.doMap error:", err)
			}
		} else {
			// build the encoder
			encoder = Wk.createEncoder(kvpair[0])
			// do the map
			err := encoder.Encode(kvpair)
			if err != nil {
				log.Println("Wk.doMap error:", err)
			}
		}
	}
	Wk.closeMapOutputs()
}

func (Wk *Worker) closeMapOutputs() {
	for _, fd := range Wk.MapKeysFiles {
		fd.Close()
	}
}

func (Wk *Worker) writeFilesToDFS() {
	files, _ := ioutil.ReadDir(Local)
	// filenameList := []string{}
	// puttimeList := []time.Time{}
	for _, f := range files {
		filename := Local + f.Name()
		// filename := Wk.FilenamePrefix + "_" + key
		// time.Sleep(1000000)
		data, _ := ioutil.ReadFile(filename)
		outFilename := FileSysLocation + strings.Replace(filename, Local, "", -1)
		file := FileIn{
			FileName: outFilename,
			Data:     data,
			Append:   true,
		}
		Wk.FileNode.PutFileRemoteCall(file)
		// filenameList = append(filenameList, file.FileName)
		// puttimeList = append(puttimeList, time.Now())
		// if Wk.isLocalMaster() {
		// 	Wk.FileNode.PutInfoBroadcast(file.FileName, time.Now())
		// }
	}
	// Wk.FileNode.PutInfoBroadcastList(filenameList, puttimeList)
}

func (Wk *Worker) createEncoder(Key string) *json.Encoder {
	// encoder := *json.Encoder{}
	fileName := Wk.FilenamePrefix + "_" + Key
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Println("Wk.createEncoder error:", err)
	}
	encoder := json.NewEncoder(fd)
	Wk.Timer.Lock()
	Wk.MapKeysFiles[Key] = fd
	Wk.MapKeysEncoders[Key] = encoder
	Wk.Timer.Unlock()
	return encoder
}

func (Wk *Worker) FinishPhase(deleteInput int) {
	Wk.Timer.Lock()
	Wk.phase = 0
	Wk.Timer.Unlock()
	if deleteInput == 1 {
		os.RemoveAll(Local)
		os.Mkdir(Local, 0777)
	}
	log.Printf("Phase finished.")
}

func (Sw *ServiceWorker) FinishPhase(input interface{}, response *bool) error {
	deleteInput, _ := input.(int)
	Sw.Wk.FinishPhase(deleteInput)
	*response = true
	return nil
}

func (Sw *ServiceWorker) GetFilenames(input *bool, response *[]string) error {
	allFilenames := []string{}
	Sw.Wk.FileNode.timer.Lock()
	for k, _ := range Sw.Wk.FileNode.FileLocations {
		allFilenames = append(allFilenames, k)
	}
	Sw.Wk.FileNode.timer.Unlock()
	*response = allFilenames
	return nil
}

func (Wk *Worker) PutFilesOnQueue(filenames []string) {
	// for Wk.phase == 0 {
	// 	time.Sleep(1000)
	// }
	log.Println(Wk.fileQueue)
	Wk.Timer.Lock()
	for _, filename := range filenames {
		log.Println("Put file on queue: ", filename)
		Wk.fileQueue = append(Wk.fileQueue, filename)
	}
	Wk.Timer.Unlock()
}

func (Sw *ServiceWorker) PutFilesOnQueue(filenames interface{}, response *bool) error {
	fnames, _ := filenames.([]string)
	Sw.Wk.PutFilesOnQueue(fnames)
	*response = true
	return nil
}

func (Wk *Worker) getMapFunc(initInfo InitIn) func(string, string) [][2]string {
	PutFile(initInfo.Exe, initInfo.ExeFile)
	// get the mapfunc
	plug, err := plugin.Open(initInfo.Exe)
	if err != nil {
		log.Println(err)
	}
	Func, err := plug.Lookup("MapFunc")
	if err != nil {
		log.Println(err)
	}
	var MapFunc func(string, string) [][2]string
	MapFunc, ok := Func.(func(string, string) [][2]string)
	if !ok {
		log.Println("unexpected type from module symbol")
	}
	return MapFunc
}

func (Wk *Worker) getReduceFunc(initInfo InitIn) func(string, []string) string {
	PutFile(initInfo.Exe, initInfo.ExeFile)
	// get the reducefunc
	plug, err := plugin.Open(initInfo.Exe)
	if err != nil {
		log.Println(err)
	}
	Func, err := plug.Lookup("ReduceFunc")
	if err != nil {
		log.Println(err)
	}
	var ReduceFunc func(string, []string) string
	ReduceFunc, ok := Func.(func(string, []string) string)
	if !ok {
		log.Println("unexpected type from module symbol")
	}
	return ReduceFunc
}

func (Wk *Worker) notifyFinish() {
	if len(Wk.finishedFileQueue) > 0 {
		log.Println("length of the finishedfilequeue", len(Wk.finishedFileQueue))
		if Wk.isLocalMaster() {
			Mr.ReceiveFinish(Wk.finishedFileQueue)
			Wk.finishedFileQueue = []string{}
			return
		}
		// go func() {
		log.Println("start notify finish")
		response := false
		go TCPDialWrapper(Wk.MasterIP, Wk.MasterPort, "ServiceMaster.ReceiveFinish", Wk.finishedFileQueue, response)
		log.Println("end notify finish")
		Wk.finishedFileQueue = []string{}
		// }()
	}
}

func (Wk *Worker) GetFile(filename string) {
	log.Println(filename)
	targetNodes, _ := Wk.FileNode.GetFileContainer(filename)
	// FileLocations[filename][0]
	// targetNode := getWorkerFromId(targetId)
	outch := GetFileCallOneServer(filename, targetNodes[0])
	for v := range outch {
		PutFile(filename, v.Data)
		return
	}
}

func (Sw *ServiceWorker) GetFile(filename interface{}, response *bool) error {
	fname, _ := filename.(string)
	Sw.Wk.GetFile(fname)
	*response = true
	return nil
}

func (Wk *Worker) Printqueue() {
	Wk.Timer.Lock()
	fmt.Println("============Worker's File List============")
	fmt.Println("============phase: ", Wk.phase, "============")
	for _, file := range Wk.fileQueue {
		fmt.Println("File [", file, "] is stored on the queue\n")
	}
	fmt.Println("=========================================")
	Wk.Timer.Unlock()
}
