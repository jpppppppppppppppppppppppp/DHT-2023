package src

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	PingwaitTime = 100 * time.Millisecond
	MaintainTime = 20 * time.Millisecond
)

func gethash(str string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(str))
	return (&big.Int{}).SetBytes(hash.Sum(nil))
}

func init() {
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

const hashSize int = 160
const successorSize int = 5

type NodeInformation struct {
	Addr   string
	HashId *big.Int
}
type Node struct {
	Addr       NodeInformation
	online     bool
	onlineLock sync.RWMutex

	listener        net.Listener
	server          *rpc.Server
	data            map[string]string
	dataLock        sync.RWMutex
	fingertable     [hashSize]NodeInformation
	tableLock       sync.RWMutex
	successorList   [successorSize]NodeInformation
	successorLock   sync.RWMutex
	predecessor     NodeInformation
	predecessorLock sync.RWMutex
	quit            chan bool
	fixing          int
}

type Pair struct {
	Key   string
	Value string
}

func GetClient(addr string) (*rpc.Client, error) {
	var client *rpc.Client
	var err error
	if addr == "" {
		err = errors.New("Address empty")
		return client, err
	}
	conn, err := net.DialTimeout("tcp", addr, PingwaitTime)
	if err != nil {
		logrus.Error("[error] Dialing error: ", addr, err)
		return client, err
	}
	client = rpc.NewClient(conn)
	return client, err
}

func RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	client, err := GetClient(addr)
	if err != nil {
		logrus.Error("[error] GetClient error: ", addr, err)
		return err
	}
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("[error] Call error: ", addr, method, args, reply, err)
	} else {
		logrus.Infoln("[Success] Call: ", addr, method)
	}
	return err
}

func (node *Node) Init(addr string) {
	node.Addr.Addr = addr
	node.Addr.HashId = gethash(addr)
	fmt.Println(node.Addr)
	node.onlineLock.Lock()
	defer node.onlineLock.Unlock()
	node.online = false

	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.data = make(map[string]string)
	node.quit = make(chan bool, 1)
	node.fixing = 1
}
func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		logrus.Error("[error] Register error: ", node.Addr.Addr, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.Addr.Addr)
	if err != nil {
		logrus.Error("[error] TCP listener error: ", node.Addr.Addr, err)
		return
	}
	logrus.Infoln("[Success] Run: ", node.Addr.Addr)
	go func() {
		for node.online {
			select {
			case <-node.quit:
				return
			default:
				conn, err := node.listener.Accept()
				if err != nil {
					logrus.Error("[error] Accept error: ", err)
					logrus.Info("[end] Run end: ", node.Addr)
					return
				}
				go node.server.ServeConn(conn)
			}
		}
		logrus.Info("[end] Run end: ", node.Addr)
	}()
	node.onlineLock.Lock()
	defer node.onlineLock.Unlock()
	node.online = true
}
func (node *Node) Create() {
	node.tableLock.Lock()
	node.fingertable[0].Addr = node.Addr.Addr
	node.fingertable[0].HashId = gethash(node.fingertable[0].Addr)
	node.tableLock.Unlock()
	logrus.Info("[Lock]successorLock:", node.Addr)
	node.successorLock.Lock()
	node.successorList[0].Addr = node.Addr.Addr
	node.successorList[0].HashId = gethash(node.successorList[0].Addr)
	node.successorLock.Unlock()
	node.Mantain()
}

var base = big.NewInt(2)
var mod = new(big.Int).Exp(base, big.NewInt(160), nil)

func (node *Node) Check() bool {
	if !node.online {
		return true
	}
	var suc, pre NodeInformation
	RemoteCall(node.successorList[0].Addr, "Node.RPCGetPredecessor", "", &suc)
	RemoteCall(node.predecessor.Addr, "Node.RPCGetFirstSuccessor", "", &pre)
	if suc.HashId.Cmp(node.Addr.HashId) != 0 {
		return false
	}
	if pre.HashId.Cmp(node.Addr.HashId) != 0 {
		return false
	}
	return true
}
func (node *Node) Mantain() { //不知道写啥，只知道是每个周期都要干的事情
	go func() {
		for node.online {
			time.Sleep(MaintainTime)
			node.Stablize()
		}
		logrus.Info("[end]Maintain end: ", node.Addr)
	}()

	go func() {
		for node.online {
			time.Sleep(MaintainTime)
			node.FixFinger()
		}
		logrus.Info("[end]Finger fix end: ", node.Addr)
	}()

	go func() {
		for node.online {
			time.Sleep(MaintainTime)
			node.ChangePredecessor()
		}
		logrus.Info("[end]ChangePredecessor end: ", node.Addr)
	}()
}
func (node *Node) Stablize() error {
	logrus.Info("[Before] Stablize: ", node.Addr, node.successorList, node.predecessor)
	var suc, pre NodeInformation
	node.RPCGetFirstSuccessor("", &suc)
	err := RemoteCall(suc.Addr, "Node.RPCGetPredecessor", "", &pre)
	if err != nil {
		logrus.Error("[error] Stabilize error: ", err)
		return err
	}
	succ := suc
	if pre.Addr != "" && in(node.Addr.HashId, suc.HashId, pre.HashId, false, false) {
		suc = pre
	}
	logrus.Info("[Lock]successorLock:", node.Addr)
	var tempsuc [successorSize]NodeInformation
	err = RemoteCall(suc.Addr, "Node.RPCGetSuccessor", "", &tempsuc)
	if err != nil {
		logrus.Error("[error] Stablize error: ", err)
		return err
	}
	node.successorLock.Lock()
	node.successorList[0].Addr = suc.Addr
	node.successorList[0].HashId = gethash(node.successorList[0].Addr)
	for i := 1; i < successorSize; i++ {
		node.successorList[i].Addr = tempsuc[i-1].Addr
		node.successorList[i].HashId = gethash(node.successorList[i].Addr)
	}
	node.successorLock.Unlock()
	node.tableLock.Lock()
	node.fingertable[0].Addr = suc.Addr
	node.fingertable[0].HashId = gethash(node.fingertable[0].Addr)
	node.tableLock.Unlock()
	err = RemoteCall(suc.Addr, "Node.RPCNotify", node.Addr, nil)
	if err != nil {
		logrus.Error("[error] Stablize error: ", err)
	}
	logrus.Info("[running] Stablize: ", node.Addr, node.predecessor, node.successorList, succ, pre)
	return err
}

func (node *Node) FixFinger() error {
	logrus.Info("[Before] Finger fix: ", node.Addr, node.successorList, node.predecessor)
	ind := node.fixing
	var reply NodeInformation
	temp := new(big.Int).Exp(base, big.NewInt(int64(ind)), nil)
	ttemp := new(big.Int).Add(node.Addr.HashId, temp)
	tttemp := new(big.Int).Mod(ttemp, mod)
	node.RPCFindSuccessor(NodeInformation{"", tttemp}, &reply)
	node.tableLock.Lock()
	node.fingertable[ind].Addr = reply.Addr
	node.fingertable[ind].HashId = gethash(node.fingertable[ind].Addr)
	node.tableLock.Unlock()
	node.fixing++
	if node.fixing == 160 {
		node.fixing = 1
	}
	logrus.Info("[running] Finger fix: ", node.Addr, " ind: ", ind, " temp: ", tttemp, " reply: ", reply, node.successorList, node.predecessor)
	return nil
}

func (node *Node) ChangePredecessor() error {
	if node.predecessor.Addr != "" && !node.Ping(node.predecessor.Addr) {
		node.predecessorLock.Lock()
		node.predecessor.Addr = ""
		node.predecessorLock.Unlock()
	}
	return nil
}

func (node *Node) RPCPing(_ string, _ *struct{}) error {
	return nil
}

func (node *Node) RPCNotify(addr NodeInformation, _ *struct{}) error {
	if node.predecessor.Addr == "" || in(node.predecessor.HashId, node.Addr.HashId, addr.HashId, false, false) {
		logrus.Info("[Lock]predecessorLock:", node.Addr)
		node.predecessorLock.Lock()
		node.predecessor.Addr = addr.Addr
		node.predecessor.HashId = gethash(node.predecessor.Addr)
		node.predecessorLock.Unlock()
		logrus.Info("[running] Notify running: ", node.Addr, node.predecessor, node.successorList)
	}
	return nil
}

func (node *Node) RPCGetFirstSuccessor(_ string, reply *NodeInformation) error {
	for i := 0; i < successorSize; i++ {
		if node.Ping(node.successorList[i].Addr) {
			*reply = node.successorList[i]
			logrus.Info("[Success]GetFirstSuccessor:", node.Addr, "->", i, "->", node.successorList[i])
			return nil
		}
	}
	return errors.New("[error] Can't find first successor")
}

func (node *Node) RPCGetSuccessor(_ string, reply *[successorSize]NodeInformation) error {
	logrus.Info("[Lock]successorRLock:", node.Addr)
	node.successorLock.RLock()
	*reply = node.successorList
	node.successorLock.RUnlock()
	return nil
}

func (node *Node) RPCGetPredecessor(_ string, reply *NodeInformation) error {
	logrus.Info("[Lock]predecessorRLock:", node.Addr)
	node.predecessorLock.RLock()
	*reply = node.predecessor
	node.predecessorLock.RUnlock()
	return nil
}

func (node *Node) RPCFindSuccessor(addr NodeInformation, reply *NodeInformation) error {
	var pre, suc NodeInformation
	err := node.RPCGetFirstSuccessor("", &suc)
	if err != nil {
		logrus.Error("[error] FindSuccessor when RPCGetFirstSuccessor error: ", node.Addr.Addr, " ", err)
		return err
	}
	if in(node.Addr.HashId, suc.HashId, addr.HashId, false, true) {
		*reply = suc
		return nil
	}
	err = node.RPCFindPredecessor(addr, &pre)
	if err != nil {
		logrus.Error("[error] FindSuccessor when RPCFindPredecessor error: ", node.Addr.Addr, " ", addr, " ", err)
		return err
	}
	return RemoteCall(pre.Addr, "Node.RPCGetFirstSuccessor", "", reply)

}
func (node *Node) RPCFindPredecessor(addr NodeInformation, reply *NodeInformation) error {
	var tempsuc NodeInformation
	err := node.RPCGetFirstSuccessor("", &tempsuc)
	if err != nil {
		return err
	}
	if in(node.Addr.HashId, tempsuc.HashId, addr.HashId, false, true) {
		*reply = node.Addr
		return nil
	}
	var tar NodeInformation
	node.RPCFindClosePrecedingFinger(addr, &tar)
	var suc NodeInformation
	err = RemoteCall(tar.Addr, "Node.RPCGetFirstSuccessor", "", &suc)
	if err != nil {
		logrus.Error("[error] RPCFindPredecessor when RPCGetFirstSuccessor error: ", node.Addr.Addr, " ", tar.Addr, " ", err)
		return err
	}
	for !in(tar.HashId, suc.HashId, addr.HashId, false, true) {
		err = RemoteCall(tar.Addr, "Node.RPCFindClosePrecedingFinger", addr, &tar)
		if err != nil {
			logrus.Error("[error] RPCFindPredecessor when FindClosePrecedingFinger error: ", node.Addr.Addr, " ", tar.Addr, " ", err)
			return err
		}
		err = RemoteCall(tar.Addr, "Node.RPCGetFirstSuccessor", "", &suc)
		if err != nil {
			logrus.Error("[error] RPCFindPredecessor when GetSuccessor error: ", node.Addr.Addr, " ", tar.Addr, " ", err)
			return err
		}
	}
	*reply = tar
	return nil
}
func (node *Node) RPCFindClosePrecedingFinger(addr NodeInformation, reply *NodeInformation) error {
	node.tableLock.RLock()
	defer node.tableLock.RUnlock()
	for i := hashSize - 1; i >= 0; i-- {
		if node.fingertable[i].Addr != "" && in(node.Addr.HashId, addr.HashId, node.fingertable[i].HashId, false, false) {
			*reply = node.fingertable[i]
			return nil
		}
	}
	*reply = node.Addr
	return nil
}
func (node *Node) Ping(addr string) bool {
	err := RemoteCall(addr, "Node.RPCPing", "", nil)
	if err != nil {
		return false
	} else {
		return true
	}
}
func (node *Node) Join(addr string) (check bool) {
	fmt.Println(node.Addr.Addr, addr)
	if !node.Ping(addr) {
		logrus.Error("[error] Join: addr shutdown", node.Addr.Addr, addr)
		return false
	}
	var reply NodeInformation
	err := RemoteCall(addr, "Node.RPCFindSuccessor", node.Addr, &reply)
	if err != nil {
		logrus.Error("[error] Join: ", node.Addr.Addr, addr, err)
		return false
	}
	logrus.Info("[Success] Join: ", node.Addr.Addr, " ", reply.Addr)
	logrus.Info("[Lock]predecessorLock:", node.Addr)
	node.predecessorLock.Lock()
	node.predecessor.Addr = ""
	node.predecessor.HashId = gethash(node.predecessor.Addr)
	node.predecessorLock.Unlock()
	logrus.Info("[Lock]successorLock:", node.Addr)
	var tempsuc [successorSize]NodeInformation
	err = RemoteCall(reply.Addr, "Node.RPCGetSuccessor", "", &tempsuc)
	if err != nil {
		return false
	}
	node.successorLock.Lock()
	node.successorList[0].Addr = reply.Addr
	node.successorList[0].HashId = gethash(node.successorList[0].Addr)
	for i := 1; i < successorSize; i++ {
		node.successorList[i].Addr = tempsuc[i-1].Addr
		node.successorList[i].HashId = gethash(node.successorList[i].Addr)
	}
	node.successorLock.Unlock()
	node.Mantain()
	return true
}
func (node *Node) Quit() {
	if !node.online {
		return
	}
	node.quit <- true
	err := node.listener.Close()
	if err != nil {
		logrus.Error("[error] Quit error: ", node.Addr.Addr, err)
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	logrus.Info("[Success] Quit: ", node.Addr.Addr)
	node.quit = make(chan bool, 1)
	logrus.Info("[Final] From: ", node.Addr.Addr, " Successor: ", node.successorList, " Predecessor: ", node.predecessor.Addr)
}
func (node *Node) ForceQuit() {
	if !node.online {
		return
	}
	node.quit <- true
	err := node.listener.Close()
	if err != nil {
		logrus.Error("[error] Quit error: ", node.Addr.Addr, err)
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	logrus.Info("[Success] Quit: ", node.Addr.Addr)
	node.quit = make(chan bool, 1)
}
func (node *Node) Put(value string, key string) (check bool) { return }
func (node *Node) Get(key string) (check bool, value string) { return }
func (node *Node) Delete(key string) (check bool)            { return }
func in(lhs, rhs, what *big.Int, closeleft, closeright bool) bool {
	switch rhs.Cmp(lhs) {
	case 1:
		if closeleft && lhs.Cmp(what) == 0 {
			return true
		} else if closeright && rhs.Cmp(what) == 0 {
			return true
		} else {
			return (rhs.Cmp(what) == 1) && (what.Cmp(lhs) == 1)
		}
	case -1:
		if closeleft && lhs.Cmp(what) == 0 {
			return true
		} else if closeright && rhs.Cmp(what) == 0 {
			return true
		} else {
			return (rhs.Cmp(what) == 1) || (what.Cmp(lhs) == 1)
		}
	default:
		return true
	}
}
