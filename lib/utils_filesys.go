package lib

import (
	// "errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

func makeDirectories(fileName string) {
	folders := strings.Split(fileName[1:], "/")
	path := ""
	for _, folder := range folders[:len(folders)-1] {
		path = path + "/" + folder
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			os.Mkdir(path, 0777)
		}
	}
}

func getHostName(ID int) string {
	if ID >= 0 && ID < 9 {
		return "fa19-cs425-g62-0" + strconv.Itoa(ID+1) + ".cs.illinois.edu"
	} else if ID == 9 {
		return "fa19-cs425-g62-" + strconv.Itoa(ID+1) + ".cs.illinois.edu"
	} else {
		log.Fatal("ID can only be from 0 to 9.\n")
	}
	return ""
}

// Set Difference: A - B
func Difference(a []TargetNode, b []int) (diff []TargetNode) {
	m := make(map[int]bool)
	for _, item := range b {
		m[item] = true
	}
	for _, item := range a {
		if _, ok := m[item.ID]; !ok {
			diff = append(diff, item)
		}
	}
	return
}

// Exists reports whether the named file or directory exists.
func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// hash -- use hash function to find the location
func hash(filename string) int {
	h := fnv.New32a()
	h.Write([]byte(filename))
	hashNum := h.Sum32()
	index := int(hashNum) % NumNodes
	if index < 0 {
		index = -index
	}
	return index
}

func mergePutInfoChannels(chs []chan ResposePutTime) chan ResposePutTime {
	out := make(chan ResposePutTime)
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(c chan ResposePutTime) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func merge(chs []chan ResposeFileIn) chan ResposeFileIn {
	out := make(chan ResposeFileIn)
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(c chan ResposeFileIn) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func mergeGetChannels(chs []chan ResponseGet) chan ResponseGet {
	out := make(chan ResponseGet)
	var wg sync.WaitGroup
	wg.Add(1)
	for _, ch := range chs {
		go func(c chan ResponseGet) {
			for v := range c {
				// Make sure that the function does close the channel
				//_, ok := (out <- v)
				// If we can recieve on the channel then it is NOT closed
				//if ok {
				out <- v
				//}
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func mergeDelChannels(chs []chan ResponseDel) chan ResponseDel {
	out := make(chan ResponseDel)
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(c chan ResponseDel) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func mergeFindChannels(chs []chan ResponseFind) chan ResponseFind {
	out := make(chan ResponseFind)
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(c chan ResponseFind) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// check whether the node with ID is online
func (fnode *FilesysNode) checkStatus(ID int) int {
	// TODO: modify here
	hostname := getHostName(ID)
	IP := GetIP(hostname)
	port := strconv.Itoa(1234)
	fnode.MemberNode.timer.Lock()
	// IP := Introducerip
	// port := strconv.Itoa(1234 + ID)
	for index, entry := range fnode.MemberNode.memList {
		if entry.IP == IP && entry.Port == port && entry.Status == Alive {
			fnode.MemberNode.timer.Unlock()
			return index
		}
	}
	fnode.MemberNode.timer.Unlock()
	return -1
}

func (fnode *FilesysNode) GetActiveNodes() []TargetNode {
	retNodes := []TargetNode{}
	for i := 0; i < NumNodes; i++ {
		idx := i % NumNodes
		memListID := fnode.checkStatus(idx)
		if memListID != -1 {
			retNodes = append(retNodes, TargetNode{
				IP:   fnode.MemberNode.memList[memListID].IP,
				Port: fnode.MemberNode.memList[memListID].Port,
				ID:   idx,
			})
			log.Printf("Active node: ip %s and port %s", fnode.MemberNode.memList[memListID].IP, fnode.MemberNode.memList[memListID].Port)
		}
	}
	return retNodes
}

// GetFileContainer finds the node to store or stores file
func (fnode *FilesysNode) GetFileContainer(filename string) ([]TargetNode, []int) {
	firstIndex := hash(filename)
	// log.Println("first add to ", firstIndex)
	// assign retNodes
	num := 0
	retNodes := []TargetNode{}
	retIDs := []int{}
	for i := 0; i < NumNodes; i++ {
		idx := (firstIndex + i) % NumNodes
		memListID := fnode.checkStatus(idx)
		if memListID != -1 {
			num++
			retNodes = append(retNodes, TargetNode{
				IP:   fnode.MemberNode.memList[memListID].IP,
				Port: fnode.MemberNode.memList[memListID].Port,
				ID:   idx,
			})
			retIDs = append(retIDs, idx)
			//log.Printf("Choose ip %s and port %s", fnode.MemberNode.memList[memListID].IP, fnode.MemberNode.memList[memListID].Port)
			if num == NumReplica {
				return retNodes, retIDs
			}
		}
	}
	return retNodes, retIDs
}

func PutInfoCallOneServer(putTime PutTime, targetNode TargetNode) chan ResposePutTime {
	ch := make(chan ResposePutTime)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client, err := rpc.DialHTTP("tcp", targetNode.IP+":"+targetNode.Port)
		if err != nil {
			log.Fatal("dialing error:", err)
			wg.Done()
			return
		}
		// Synchronous call
		args := &putTime
		response := ResposePutTime{
			FileName: putTime.FileName,
			ID:       targetNode.ID,
		}
		err = client.Call("Service.RecordPutTime", args, &response)
		if err != nil {
			log.Fatal("Record put time call error:", err)
			wg.Done()
			return
		}
		ch <- response
		wg.Done()
		client.Close()
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func PutFileCallOneServer(file FileIn, targetNode TargetNode) chan ResposeFileIn {
	ch := make(chan ResposeFileIn)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client, err := rpc.DialHTTP("tcp", targetNode.IP+":"+targetNode.Port)
		if err != nil {
			log.Fatal("dialing error:", err)
			wg.Done()
			return
		}
		// Synchronous call
		args := &file
		response := ResposeFileIn{
			FileName: file.FileName,
			ID:       targetNode.ID,
		}
		err = client.Call("Service.PutFile", args, &response)
		if err != nil {
			log.Fatal("Put file call error:", err)
			wg.Done()
			return
		}
		ch <- response
		wg.Done()
		client.Close()
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// GetFileCallOneServer -- fetch file on corresponding nodes
func GetFileCallOneServer(fileName string, targetNode TargetNode) chan ResponseGet {
	ch := make(chan ResponseGet)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client, err := rpc.DialHTTP("tcp", targetNode.IP+":"+targetNode.Port)
		if err != nil {
			log.Fatal("dialing error:", err)
			wg.Done()
			return
		}
		// Synchronous call
		response := ResponseGet{
			FileName: fileName,
			ID:       targetNode.ID,
		}
		// log.Println("call Service.GetFile")
		err = client.Call("Service.GetFile", &fileName, &response)
		// log.Println("Get Service.GetFile results")
		if err != nil {
			log.Print("Get file call error:", err)
			wg.Done()
			return
		}
		ch <- response
		wg.Done()
		client.Close()
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// DelFileCallOneServer -- Delete file on corresponding nodes
func DelFileCallOneServer(fileName string, targetNode TargetNode) chan ResponseDel {
	ch := make(chan ResponseDel)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client, err := rpc.DialHTTP("tcp", targetNode.IP+":"+targetNode.Port)
		if err != nil {
			log.Printf("dialing error:", err)
			wg.Done()
			return
		}
		// Synchronous call
		response := ResponseDel{
			FileName: fileName,
			ID:       targetNode.ID,
		}
		err = client.Call("Service.DeleteFile", &fileName, &response)
		if err != nil {
			log.Printf("Delete file call error:", err)
			wg.Done()
			return
		}
		ch <- response
		wg.Done()
		client.Close()
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch

}

// FindFileCallOneServer -- Find a file on all nodes
func FindFileOneServer(fileName string, targetNode TargetNode) chan ResponseFind {
	ch := make(chan ResponseFind)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client, err := rpc.DialHTTP("tcp", targetNode.IP+":"+targetNode.Port)
		if err != nil {
			log.Fatal("dialing error:", err)
			wg.Done()
			return
		}
		// Synchronous call
		response := ResponseFind{
			FileName: fileName,
			ID:       targetNode.ID,
			IP:       targetNode.IP,
			Port:     targetNode.Port,
		}
		err = client.Call("Service.ListFile", &fileName, &response)
		if err != nil {
			log.Fatal("Finding file call error:", err)
			wg.Done()
			return
		}
		ch <- response
		wg.Done()
		client.Close()
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch

}

func (fnode *FilesysNode) PrintFile() {
	fnode.timer.Lock()
	fmt.Println("============", fnode.ID, "'s File List============")
	for key, value := range fnode.FileLocations {
		log.Println("File [", key, "] is stored ", value, "\n")
	}
	for key, value := range fnode.FilePutTime {
		log.Println("File [", key, "] is stored at ", value, "\n")
	}
	fmt.Println("=========================================")
	fnode.timer.Unlock()
}
