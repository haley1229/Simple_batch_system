package lib

import (
	"fmt"
	"log"
	"net"

	"os"
	"sort"

	"encoding/json"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var introducer = "fa19-cs425-g62-05.cs.illinois.edu"

var Introducerip = "172.22.156.206"//"192.168.1.3" //TODO

var introducerPort = "1234"

// var introducerPort = flag.String("port", "1234", "port to listen")

// TIMEfail is for the time of failure
var TIMEfail = 1.2

// TIMEcleanup is for the time to clean up
var TIMEcleanup = 0.4

// TIMEheartbeat is for Heartbeat interval
var TIMEheartbeat time.Duration = 400000000 // 0.6s

// Message Loss Rate
var messageLossRate = 0

// The struct for the info of the membership list
type memEntry struct {
	Name string
	// Ip_array  [4]int
	IP        string
	Port      string
	Heartbeat int
	Time      time.Time
	Status    int
}

// The struct for the message to send
type msgEntry struct {
	Type      int
	IP        string
	Port      string
	Heartbeat int
	Status    int
}

const (
	Alive  = iota
	Failed = iota
)

const (
	// Introducer index
	Introducer = iota
	// Server index
	Server = iota
)

const (
	join      = iota
	heartbeat = iota
)

// The struct for the neighbor info
type neighbor struct {
	Name string
	ID   int
}

// Node stores the info of each node
type Node struct {
	Name     string
	ID       int //position among all vms
	IP       string
	Port     string
	Status   int
	neighbor []neighbor
	memList  []memEntry
	delIndex []int
	timer    sync.Mutex
}

// Msg stores the info in the message
type Msg struct {
	MsgType int
	MsgList []msgEntry
}

var maxNbr = 3

// UpdateID updates the id in membership list
func (node *Node) UpdateID() {
	for index, entry := range node.memList {
		if entry.IP == node.IP && entry.Port == node.Port {
			node.ID = index
			return
		}
	}
	panic("ERR: current IP not found in the memberlist")
}

func (node *Node) assignNeighbor() (err error) {
	// fmt.Println("Assign new neighbor starts \n")
	cnt := 0
	stillAlive := []neighbor{}

	list := node.memList
	for i := 1; i < len(list); i++ {
		cur := (node.ID + i) % len(list)
		// neighbor cannot be itself
		if cur == node.ID {
			break
		}
		if list[cur].Status == Failed {
			continue
		}
		stillAlive = append(stillAlive, neighbor{list[cur].Name, cur})
		cnt++
		if cnt >= maxNbr {
			break
		}
	}
	node.neighbor = stillAlive

	return nil
}

func (node *Node) pingIntroducerToJoin() (err error) {
	service := GetIP(introducer) + ":" + introducerPort
	// service := Introducerip + ":" + introducerPort //TODO: GetIP(introducer) + ":" + introducerPort//GetIP(introducer) + ":" + introducerPort
	log.Println("Pinging introducer" + service + "to join")
	conn, err := net.Dial("udp", service)
	if err != nil {
		log.Fatal("Unable to dial to the introducer : ", err)
		return
	}
	defer conn.Close()
	msgentry := msgEntry{
		Type:      nameToType(node.Name),
		IP:        node.IP,
		Port:      node.Port,
		Heartbeat: 1,
		Status:    Alive,
	}
	msg := Msg{
		MsgType: join,
		MsgList: []msgEntry{msgentry},
	}
	fmt.Println(msg)
	jsonMsg, _ := json.Marshal(msgCompression(msg))
	conn.Write(jsonMsg)
	return nil
}

// This function is used to dial to other nodes
func (node *Node) dialNode(msg Msg, nbr neighbor) {
	service := node.memList[nbr.ID].IP + ":" + node.memList[nbr.ID].Port
	// fmt.Println("start to heartbeat to: ", service)
	conn, err := net.Dial("udp", service)
	if err != nil {
		log.Fatal("Unable to dial to the introducer : ", err)
		return
	}
	defer conn.Close()
	jsonMsg, _ := json.Marshal(msgCompression(msg))
	conn.Write(jsonMsg)
	return
}

// KeepAlive is used by all alive nodes to periotically send mem_list to others
func (node *Node) KeepAlive() (err error) {
	for {
		time.Sleep(TIMEheartbeat)
		node.timer.Lock()
		// if failed, stop heartbeat
		if node.Status == Failed {
			log.Printf("Node %s has failed, cannot send keep-alive msg", node.Name)
			node.timer.Unlock()
			return
		}
		// update current entry
		if len(node.memList) <= node.ID {
			continue
		}
		// check all the mem lists and make changes
		node.HeartbeatUpdate()
		// Send keep-alive msg to all its nbrs
		for _, nbr := range node.neighbor {
			if node.memList[nbr.ID].Status == Failed {
				continue
			}
			msg := Msg{
				MsgType: heartbeat,
				MsgList: createMsgList(node.memList),
			}
			if rand.Intn(100) >= messageLossRate {
				node.dialNode(msg, nbr)
			}
		}
		node.timer.Unlock()
	}
	return nil
}

// LeaveGroup is used by an alive node to leave the group
func (node *Node) LeaveGroup() (err error) {
	if node.Status == Failed {
		log.Printf("Node %s has failed, cannot send keep-alive msg", node.Name)
		return
	}
	for {
		node.timer.Lock()
		node.Status = Failed
		node.memList[node.ID].Status = Failed
		node.timer.Unlock()

		time.Sleep(TIMEheartbeat)
		// update current entry
		if len(node.memList) <= node.ID {
			continue
		}
		node.timer.Lock()
		// check all the mem lists and make changes
		node.HeartbeatUpdate()

		// Send keep-alive msg to all its nbrs
		for _, nbr := range node.neighbor {
			if node.memList[nbr.ID].Status == Failed {
				continue
			}
			msg := Msg{
				MsgType: heartbeat,
				MsgList: createMsgList(node.memList),
			}
			node.dialNode(msg, nbr)
			log.Println("Sent keep alive message to :" + nbr.Name)
			log.Println(msg)
		}
		node.timer.Unlock()
		break
	}
	node.neighbor = []neighbor{}
	node.memList = []memEntry{}
	// update membership list to the log when node left
	log.Println("Node left, updated membership list:")
	log.Println(node.memList)
	return nil
}

// JoinNewNode renews the membership list when a new node joins
func (node *Node) JoinNewNode() (err error) {
	// get the IP before join
	node.timer.Lock()
	node.Status = Alive
	osName, _ := os.Hostname()
	node.IP = GetIP(osName)
	// node.IP = Introducerip //TODO: GetIP(osName)
	if node.Name == "server" {
		fmt.Println("Starting join node ", node.Name)
		err := node.pingIntroducerToJoin()
		if err != nil {
			log.Fatal("Ping Join Failed")
		}
	} else if node.Name == "introducer" {
		mementry := memEntry{
			Name:      node.Name,
			IP:        node.IP,
			Port:      node.Port,
			Heartbeat: 1,
			Time:      time.Now(),
			Status:    Alive,
		}
		node.memList = []memEntry{mementry}
		log.Println("New node joined, updated membership list:")
		log.Println(node.memList)
	}
	node.timer.Unlock()
	return nil
}

//updateList updates list when a node fails/joins/leaves
func (node *Node) updateList(msg Msg) {
	if msg.MsgType == join {
		// check if msg IP is in mem_list
		index := nodeInList(msg.MsgList[0], node.memList)
		insertEntry := createMemEntry(msg.MsgList[0])
		if index != -1 {
			// if yes, set the memEntry.status to be alive
			node.memList[index] = insertEntry
		} else {
			// if no, create insert one new memEntry
			node.memList = append(node.memList, insertEntry)
		}
	} else if msg.MsgType == heartbeat {
		for i := 0; i < len(msg.MsgList); i++ {
			// check if msg IP is in mem_list
			index := nodeInList(msg.MsgList[i], node.memList)
			if index != -1 {
				// if yes, check different possibilities
				memUpdateCheck(&node.memList[index], &msg.MsgList[i])

			} else if msg.MsgList[i].Status == Alive {
				// if no, create insert one new memEntry
				node.memList = append(node.memList, createMemEntry(msg.MsgList[i]))
			}
		}
	}
	sort.Slice(node.memList, func(i, j int) bool {
		//node.mem_list[i].ip < node.mem_list[j].ip
		return strings.Compare(node.memList[i].IP, node.memList[j].IP) == -1
	})
	node.UpdateID()
}

// ListenToUDP takes in a udp msg
func (node *Node) ListenToUDP() (err error) {
	addr := node.IP + ":" + node.Port
	lst, err := net.ResolveUDPAddr("udp", addr)
	log.Println("Node ", addr, " is listening")

	if err != nil {
		log.Fatal("Listen to UDP:", err)
		return err
	}
	conn, _ := net.ListenUDP("udp", lst)
	defer conn.Close()

	for {
		buf := make([]byte, 4096)
		cnt, _, _ := conn.ReadFromUDP(buf[:])
		compressedMsg := []int{}
		json.Unmarshal(buf[:cnt], &compressedMsg)
		msg := msgDeCompression(compressedMsg)
		node.timer.Lock()
		if msg.MsgType == join {
			fmt.Println("Received ", cnt, " bytes")
			fmt.Println(msg)

			if node.Name == "introducer" {
				node.updateList(msg)

			} else {
				panic("ERR: servers should not receive join message")
			}
		} else if msg.MsgType == heartbeat {
			node.updateList(msg)
		}
		node.assignNeighbor()
		node.timer.Unlock()

	}

}
