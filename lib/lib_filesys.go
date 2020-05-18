package lib

import (
	"errors"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var NumNodes = 10
var NumReplica = 3

type FileIn struct {
	FileName  string
	Data      []byte
	Locations []int
	Append    bool
}

type PutTime struct {
	FileName string
	Time     time.Time
}

type ResposePutTime struct {
	FileName string
	ID       int
	Success  bool
}

type ResposeFileIn struct {
	FileName string
	ID       int
}

type ResponseGet struct {
	FileName string
	Data     []byte
	ID       int
}

type ResponseDel struct {
	FileName string
	ID       int
}

type ResponseFind struct {
	FileName string
	ID       int
	IP       string
	Port     string
	Exists   bool
}

type FilesysNode struct {
	ID         int
	MemberNode *Node
	// ActiveNodes   []int
	FileLocations map[string][]int
	FilePutTime   map[string]time.Time
	timer         sync.Mutex
}

type TargetNode struct {
	IP   string
	Port string
	ID   int
}

type Service struct {
	FileNode *FilesysNode
}

func (service *Service) RecordPutTime(putTime *PutTime, response *ResposePutTime) error {
	// used for later store
	service.FileNode.RecordPutTime(putTime.FileName, putTime.Time)
	response.Success = true
	return nil
}

func (fnode *FilesysNode) RecordPutTime(filename string, putTime time.Time) {
	fnode.timer.Lock()
	fnode.FilePutTime[filename] = putTime
	fnode.timer.Unlock()
}

func (service *Service) PutFile(file *FileIn, response *ResposeFileIn) error {
	service.FileNode.PutFile(*file)
	return nil
}

func (service *Service) GetFile(filename *string, response *ResponseGet) error {
	// log.Println("service.GetFile start read the file data")
	data, err := ioutil.ReadFile(*filename)
	// log.Println("service.GetFile finish read the file data")
	if err != nil {
		return err
	}
	response.Data = data
	return nil
}

func (service *Service) DeleteFile(filename *string, response *ResponseDel) error {
	err := os.Remove(*filename)
	if err != nil {
		return err
	}
	// used for later store
	if _, ok := service.FileNode.FileLocations[*filename]; ok {
		delete(service.FileNode.FileLocations, *filename)
	}
	return nil
}

func (service *Service) ListFile(filename *string, response *ResponseFind) error {
	if Exists(*filename) {
		response.Exists = true
	} else {
		response.Exists = false
	}
	return nil
}

func (service *Service) CheckFile(filename *string, response *bool) error {
	if Exists(*filename) {
		*response = true
	} else {
		*response = false
	}
	return nil
}

func (fnode *FilesysNode) PutInfoBroadcast(fileName string, putTime time.Time) {
	targetNodes := fnode.GetActiveNodes()
	nodesToCall := []TargetNode{}
	memberNode := fnode.MemberNode
	for i := 0; i < len(targetNodes); i++ {
		if targetNodes[i].IP == memberNode.IP && targetNodes[i].Port == memberNode.Port {
			//log.Printf("Record put file [%s] time with ID [%d].\n", fileName, targetNodes[i].ID)
			fnode.timer.Lock()
			fnode.FilePutTime[fileName] = putTime
			fnode.timer.Unlock()
		} else {
			nodesToCall = append(nodesToCall, targetNodes[i])
		}
	}
	outchs := make([]chan ResposePutTime, len(nodesToCall))
	for i, nodeToCall := range nodesToCall {
		args := PutTime{
			FileName: fileName,
			Time:     putTime,
		}
		outchs[i] = PutInfoCallOneServer(args, nodeToCall)
	}
	mergePutInfoChannels(outchs)
}

// func (fnode *FilesysNode) PutInfoBroadcastList(fileNameList []string, putTimeList []time.Time) {
// 	targetNodes := fnode.GetActiveNodes()
// 	nodesToCall := []TargetNode{}
// 	memberNode := fnode.MemberNode
// 	for i := 0; i < len(targetNodes); i++ {
// 		if targetNodes[i].IP == memberNode.IP && targetNodes[i].Port == memberNode.Port {
// 			fnode.timer.Lock()
// 			log.Printf("Record put file [%s] time with ID [%d].\n", fileName, targetNodes[i].ID)
// 			fnode.FilePutTime[fileName] = putTime
// 			fnode.timer.Unlock()
// 		} else {
// 			nodesToCall = append(nodesToCall, targetNodes[i])
// 		}
// 	}
// 	outchs := make([]chan ResposePutTime, len(nodesToCall))
// 	for i, nodeToCall := range nodesToCall {
// 		args := PutTime{
// 			FileName: fileName,
// 			Time:     putTime,
// 		}
// 		outchs[i] = PutInfoCallOneServer(args, nodeToCall)
// 	}
// 	mergePutInfoChannels(outchs)
// }

// PutFile -- put file to target node
func (fnode *FilesysNode) PutFile(file FileIn) {
	if Exists(file.FileName) == false {
		makeDirectories(file.FileName)
	}
	fnode.timer.Lock()
	localfile, error := os.OpenFile(file.FileName, os.O_RDWR|os.O_CREATE, 0600)
	if file.Append == true {
		localfile.Close()
		localfile, error = os.OpenFile(file.FileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	}
	if error != nil {
		log.Fatal("File open error:", error)
	}
	_, writeError := localfile.Write(file.Data)
	if writeError != nil {
		log.Fatal("File put error:", writeError)
	}
	localfile.Close()
	fnode.FileLocations[file.FileName] = file.Locations
	fnode.timer.Unlock()
}

func (fnode *FilesysNode) PutFileRemoteCall(file FileIn) {
	targetNodes, targetIDs := fnode.GetFileContainer(file.FileName)
	file.Locations = targetIDs
	nodesToCall := []TargetNode{}
	memberNode := fnode.MemberNode
	for i := 0; i < len(targetNodes); i++ {
		if targetNodes[i].IP == memberNode.IP && targetNodes[i].Port == memberNode.Port {
			fnode.PutFile(file)
			log.Printf("put file [%s] with ID [%d].\n", file.FileName, targetNodes[i].ID)
		} else {
			nodesToCall = append(nodesToCall, targetNodes[i])
		}
	}
	outchs := make([]chan ResposeFileIn, len(nodesToCall))
	for i, nodeToCall := range nodesToCall {
		outchs[i] = PutFileCallOneServer(file, nodeToCall)
	}
	outch := merge(outchs)

	for v := range outch {
		log.Printf("remotely put file [%s] with ID [%d].\n", v.FileName, v.ID)
	}
	fnode.timer.Lock()
	fnode.FileLocations[file.FileName] = file.Locations
	fnode.timer.Unlock()
}

func (fnode *FilesysNode) GetFileRemoteCall(fileName string) ([]byte, error) {
	targetNodes, _ := fnode.GetFileContainer(fileName)
	nodesToCall := []TargetNode{}
	memberNode := fnode.MemberNode
	for i := 0; i < len(targetNodes); i++ {
		if targetNodes[i].IP == memberNode.IP && targetNodes[i].Port == memberNode.Port {
			data, err := ioutil.ReadFile(fileName)
			if err != nil {
				log.Printf("Get warining: the file is not available\n")
				return data, nil
			} else {
				log.Printf("get file [%s] from local with ID [%d].\n", fileName, targetNodes[i].ID)
				return data, nil
			}
		} else {
			nodesToCall = append(nodesToCall, targetNodes[i])
		}
	}
	//outchs := make([]chan ResponseGet, len(nodesToCall))
	outchs := make([]chan ResponseGet, 1)
	outchs[0] = GetFileCallOneServer(fileName, nodesToCall[0])
	//for i, nodeToCall := range nodesToCall {
	//	outchs[i] = GetFileCallOneServer(fileName, nodeToCall)
	//}
	outch := mergeGetChannels(outchs)
	for v := range outch {
		log.Printf("get file [%s] with ID [%d].\n", v.FileName, v.ID)
		return v.Data, nil
	}
	return []byte(""), errors.New("Get none fron the file system")
}

func (fnode *FilesysNode) DeleteFileRemoteCall(fileName string) error {
	targetNodes, _ := fnode.GetFileContainer(fileName)
	nodesToCall := []TargetNode{}
	memberNode := fnode.MemberNode
	for i := 0; i < len(targetNodes); i++ {
		if targetNodes[i].IP == memberNode.IP && targetNodes[i].Port == memberNode.Port {
			err := os.Remove(fileName)
			if err != nil {
				log.Printf("Delete warning: local filename not exists on ID %d\n", targetNodes[i].ID)
			} else {
				log.Printf("Delete file [%s] from local with ID [%d].\n", fileName, targetNodes[i].ID)
				// used for later store
				if _, ok := fnode.FileLocations[fileName]; ok {
					delete(fnode.FileLocations, fileName)
				}
			}
		} else {
			nodesToCall = append(nodesToCall, targetNodes[i])
		}
	}
	outchs := make([]chan ResponseDel, len(nodesToCall))
	for i, nodeToCall := range nodesToCall {
		outchs[i] = DelFileCallOneServer(fileName, nodeToCall)
	}
	outch := mergeDelChannels(outchs)
	for v := range outch {
		log.Printf("Deleted file [%s] with ID [%d].\n", v.FileName, v.ID)
	}
	return errors.New("Delete all fron the file system")
}

func (fnode *FilesysNode) ListFileRemoteCall(fileName string) error {
	targetNodes, _ := fnode.GetFileContainer(fileName)
	nodesToCall := []TargetNode{}
	memberNode := fnode.MemberNode
	// var nodeList []string
	for i := 0; i < len(targetNodes); i++ {
		if targetNodes[i].IP == memberNode.IP && targetNodes[i].Port == memberNode.Port {
			if Exists(fileName) {
				log.Printf("File [%s] on ID [%d] IP [%s] Port [%s].\n", fileName, targetNodes[i].ID, targetNodes[i].IP, targetNodes[i].Port)
			}
		} else {
			nodesToCall = append(nodesToCall, targetNodes[i])
		}
	}
	outchs := make([]chan ResponseFind, len(nodesToCall))
	for i, nodeToCall := range nodesToCall {
		outchs[i] = FindFileOneServer(fileName, nodeToCall)
	}
	outch := mergeFindChannels(outchs)
	for v := range outch {
		if v.Exists {
			log.Printf("File [%s] on ID [%d] IP [%s] Port [%s].\n", fileName, v.ID, v.IP, v.Port)
		}
	}
	return nil
}

// Store --List all files being stored at one machine
func (fnode *FilesysNode) Store() {
	for key, _ := range fnode.FileLocations {
		log.Printf("File %s is stored \n", key)
	}
}

// KeepAlive is used by all alive nodes to periotically send mem_list to others
func (fnode *FilesysNode) HeartBeat() (err error) {
	for {
		time.Sleep(30 * TIMEheartbeat)
		// if failed, stop heartbeat
		fnode.MemberNode.timer.Lock()
		if fnode.MemberNode.Status == Failed {
			log.Printf("Node %s has failed, will not check filesystem condition\n", fnode.MemberNode.Name)
			fnode.MemberNode.timer.Unlock()
			return
		}
		fnode.MemberNode.timer.Unlock()
		// make the copy
		fnode.timer.Lock()
		FileLocationsCopy := map[string][]int{}
		for filename, IDs := range fnode.FileLocations {
			FileLocationsCopy[filename] = IDs
		}
		fnode.timer.Unlock()
		for filename, IDs := range FileLocationsCopy {
			activeNodes, activeIDs := fnode.GetFileContainer(filename)
			NodesToCall := Difference(activeNodes, IDs)
			// log.Println("nodes to call are: ", NodesToCall)
			data, err := ioutil.ReadFile(filename)
			if err != nil {
				continue
				// log.Printf("error: filename not exists\n")
			}
			for _, node := range NodesToCall {
				if fnode.checkStatus(node.ID) != -1 {
					// ask if it has the filename
					client, err := rpc.DialHTTP("tcp", node.IP+":"+node.Port)
					if err != nil {
						log.Printf("heartbeat dialing error:", err)
					}
					// response := false
					// err = client.Call("Service.CheckFile", &filename, &response)
					// if err != nil {
					// 	log.Printf("heartbeat Check file call error:", err)
					// }
					// // if no, then transfer the filename
					// if response == false && fnode.checkStatus(node.ID) != -1 {
					if fnode.checkStatus(node.ID) != -1 {
						file := FileIn{
							FileName:  filename,
							Data:      data,
							Locations: activeIDs,
							Append:    false,
						}
						args := &file
						response := ResposeFileIn{
							FileName: file.FileName,
							ID:       node.ID,
						}
						err = client.Call("Service.PutFile", args, &response)
						if err != nil {
							log.Printf("heartbeat Put file call error:", err)
						}
					}
					// }
					client.Close()
				}
			}
			fnode.timer.Lock()
			fnode.FileLocations[filename] = activeIDs
			fnode.timer.Unlock()
		}
	}
	return nil
}
