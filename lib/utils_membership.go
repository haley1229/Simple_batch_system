package lib

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var compressedMsgEntryLen = 8

// GetIP is a helper function to get the IP address.
func GetIP(name string) string {
	addrs, err := net.LookupHost(name)
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}
	//log.Println("My ip address is: ", addrs[0])
	return addrs[0]
}

// This function transfers the node name to int
func typeToName(stype int) string {
	if stype == Introducer {
		return "introducer"
	} else if stype == Server {
		return "server"
	} else {
		log.Fatal("No other type options.\n")
	}
	return "0"
}

// This function transfers the int node type to its name
func nameToType(name string) int {
	if name == "introducer" {
		return Introducer
	} else if name == "server" {
		return Server
	} else {
		log.Fatal("No other name options.\n")
	}
	return 0
}

// This function calculates the time cost.
func subtractTime(time1, time2 time.Time) float64 {
	diff := time2.Sub(time1).Seconds()
	return diff
}

// This function adds new node info to the membership.
func createMemEntry(input msgEntry) memEntry {
	return memEntry{
		Name:      typeToName(input.Type),
		IP:        input.IP,
		Port:      input.Port,
		Heartbeat: input.Heartbeat,
		Time:      time.Now(),
		Status:    input.Status,
	}
}

// This function generate message for each node.
func createMsgList(input []memEntry) []msgEntry {
	retEntry := []msgEntry{}
	for _, mementry := range input {
		retEntry = append(retEntry, msgEntry{
			Type:      nameToType(mementry.Name),
			IP:        mementry.IP,
			Port:      mementry.Port,
			Heartbeat: mementry.Heartbeat,
			Status:    mementry.Status,
		})
	}
	return retEntry
}

/*
return the index where mem_list[index].ip == ip
return -1 if ip not in the list
*/
func nodeInList(inputEntry msgEntry, memList []memEntry) int {
	for index, entry := range memList {
		if entry.IP == inputEntry.IP && entry.Port == inputEntry.Port {
			return index
		}
	}
	return -1
}

// This function checks the update of information of nodes in membership list.
func memUpdateCheck(nodeEntry *memEntry, queryEntry *msgEntry) {

	timeDiff := subtractTime(nodeEntry.Time, time.Now())
	heartbeatEiff := queryEntry.Heartbeat - nodeEntry.Heartbeat

	if nodeEntry.Status == Alive {
		if timeDiff <= TIMEfail && heartbeatEiff > 0 {
			nodeEntry.Heartbeat = queryEntry.Heartbeat
			nodeEntry.Time = time.Now()
			nodeEntry.Status = queryEntry.Status
		}
	}
	//  else if nodeEntry.Status == failed {
	// 	if heartbeatEiff > 0 {
	// 		nodeEntry.Heartbeat = queryEntry.Heartbeat
	// 		nodeEntry.Time = time.Now()
	// 		nodeEntry.Status = queryEntry.Status
	// 	}
	// }
}

// Use heartbeat to check failure
func (node *Node) memHeartbeatCheck(index int) {
	timeDiff := subtractTime(node.memList[index].Time, time.Now())
	if node.memList[index].Status == Alive {
		// if alive: check whether to mark it as failure
		if timeDiff > TIMEfail {
			node.memList[index].Status = Failed
			node.memList[index].Time = time.Now()
		}
	} else {
		// if failure: check whether to remove the entry
		if timeDiff > TIMEcleanup {
			node.delIndex = append(node.delIndex, index)
		}
	}
}

// HeartbeatUpdate updates node information in each heartbeat
func (node *Node) HeartbeatUpdate() {
	node.memList[node.ID].Heartbeat++
	node.memList[node.ID].Time = time.Now()
	node.delIndex = []int{}
	for index := range node.memList {
		if index != node.ID {
			node.memHeartbeatCheck(index)
		}
	}
	node.delFailedEntries()
	node.assignNeighbor()
	node.UpdateID()
}

// Delete failed nodes from the membership list.
func (node *Node) delFailedEntries() {
	if len(node.delIndex) == 0 {
		return
	}
	newMemlist := []memEntry{}
	reti := 0
	deli := 0
	for i := 0; i < len(node.memList); i++ {
		if deli < len(node.delIndex) && i == node.delIndex[deli] {
			deli++
		} else {
			newMemlist = append(newMemlist, node.memList[i])
			reti++
		}
	}
	node.memList = newMemlist
}

// IPToList change string IP to int list
func IPToList(IP string) []int {
	IPList := []int{}
	IPSplit := strings.Split(IP, ".")
	for _, value := range IPSplit {
		intValue, _ := strconv.Atoi(value)
		IPList = append(IPList, intValue)
	}
	return IPList
}

// ListToIP converts the list to array message
func ListToIP(IPList []int) string {
	if len(IPList) != 4 {
		log.Fatal("Length of the IP array should be 4.\n")
	}
	return strconv.Itoa(IPList[0]) + "." + strconv.Itoa(IPList[1]) + "." + strconv.Itoa(IPList[2]) + "." + strconv.Itoa(IPList[3])
}

// change msgentry to int array
func msgEntryCompression(msgentry msgEntry) []int {
	compressedEntry := []int{}
	intPort, _ := strconv.Atoi(msgentry.Port)
	compressedEntry = append(compressedEntry, msgentry.Type)
	compressedEntry = append(compressedEntry, IPToList(msgentry.IP)...)
	compressedEntry = append(compressedEntry, intPort)
	compressedEntry = append(compressedEntry, msgentry.Heartbeat)
	compressedEntry = append(compressedEntry, msgentry.Status)
	if len(compressedEntry) != compressedMsgEntryLen {
		log.Fatal("Length of the compressed msg array should be 8.\n")
	}
	return compressedEntry
}

// Compress the entry message
func msgEntryDeCompression(compressedList []int) msgEntry {
	return msgEntry{
		Type:      compressedList[0],
		IP:        ListToIP(compressedList[1:5]),
		Port:      strconv.Itoa(compressedList[5]),
		Heartbeat: compressedList[6],
		Status:    compressedList[7],
	}
}

// Compress the msg
func msgCompression(msg Msg) []int {
	compressedMsg := []int{}
	compressedMsg = append(compressedMsg, msg.MsgType)
	for _, msgentry := range msg.MsgList {
		compressedMsg = append(compressedMsg, msgEntryCompression(msgentry)...)
	}
	return compressedMsg
}

// Decompress the msg
func msgDeCompression(compressedMsg []int) Msg {
	msg := Msg{}
	msg.MsgType = compressedMsg[0]
	msg.MsgList = []msgEntry{}
	msgLength := (len(compressedMsg) - 1) / compressedMsgEntryLen

	for i := 0; i < msgLength; i++ {
		msg.MsgList = append(msg.MsgList, msgEntryDeCompression(compressedMsg[i*compressedMsgEntryLen+1:(i+1)*compressedMsgEntryLen+1]))
	}
	return msg
}

// PrintID prints out the ID of the node.
func (node *Node) PrintID() {
	node.timer.Lock()
	//Lock membership list before printing
	fmt.Println("============", node.Name, "'s ID============")
	if len(node.memList) > 0 {
		fmt.Println("The ID is: ", node.ID)
	} else {
		fmt.Println("No membership exists at this moment.")
	}
	fmt.Println("=========================================")
	node.timer.Unlock()
}

// PrintNeighbor prints out the neighbor list of a node.
func (node *Node) PrintNeighbor() {
	node.timer.Lock()
	//Lock membership list before printing
	fmt.Println("============", node.Name, "'s neighbor List============")
	fmt.Println(len(node.neighbor))
	fmt.Printf("|-name-|--id--|\n")
	for i := 0; i < len(node.neighbor); i++ {
		fmt.Printf("%6s|%6d|\n", node.neighbor[i].Name, node.neighbor[i].ID)
	}
	fmt.Println("=========================================")
	node.timer.Unlock()
}

// PrintMembership prints out the membership list of a node
func (node *Node) PrintMembership() {
	node.timer.Lock()
	//Lock membership list before printing
	fmt.Println("============", node.Name, "'s Membership List============")
	// log.Println("============", node.Name, "'s Membership List============")
	fmt.Println(len(node.memList))
	// log.Println(len(node.memList))
	fmt.Printf("|-----ip----|-port-|---name---|status|--hb--|\n")
	// log.Printf("|-----ip----|-port-|---name---|status|--hb--|\n")
	for i := 0; i < len(node.memList); i++ {
		// this is only for test
		// if node.memList[i].Status == failed {
		// 	panic("detected failed entry")
		// }
		fmt.Printf("|%6s|%6s|%6s|%6d|%6d|\n", node.memList[i].IP, node.memList[i].Port, node.memList[i].Name, node.memList[i].Status, node.memList[i].Heartbeat)
		// log.Printf("|%6s|%6s|%6s|%6d|%6d|\n", node.memList[i].IP, node.memList[i].Port, node.memList[i].Name, node.memList[i].Status, node.memList[i].Heartbeat)
	}
	fmt.Println("=========================================")
	// log.Println("=========================================")
	node.timer.Unlock()
}
