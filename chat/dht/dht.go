package dht

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
	PingwaitTime = 10 * time.Second
	MaintainTime = 100 * time.Millisecond
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
const successorSize int = 10

type NodeInformation struct {
	Addr   string
	HashId *big.Int
}
type Message struct {
	Timestamp time.Time
	Message   string
	From      string
}
type Node struct {
	Seed            string
	Addr            NodeInformation
	Online          bool
	onlineLock      sync.RWMutex
	Bind            string
	listener        net.Listener
	server          *rpc.Server
	data            map[string]([]Message)
	dataLock        sync.RWMutex
	backup          map[string]([]Message)
	backupLock      sync.RWMutex
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
	Value Message
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

func (node *Node) Init(bindaddr string, nameaddr string) {
	node.Addr.Addr = nameaddr
	node.Addr.HashId = gethash(nameaddr)
	node.Bind = bindaddr
	node.Seed = ""
	node.onlineLock.Lock()
	defer node.onlineLock.Unlock()
	node.Online = false

	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.data = make(map[string]([]Message))
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	node.backup = make(map[string]([]Message))
	node.quit = make(chan bool, 1)
	node.fixing = 1
}
func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		logrus.Error("[error] Register error: ", node.Bind, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.Bind)
	if err != nil {
		logrus.Error("[error] TCP listener error: ", node.Bind, err)
		return
	}
	logrus.Infoln("[Success] Run: ", node.Bind)
	go func() {
		for node.Online {
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
	node.Online = true
}
func (node *Node) Create() {
	node.Seed = fmt.Sprintf("%d", time.Now().UnixNano())
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
	if !node.Online {
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
		for node.Online {
			time.Sleep(MaintainTime)
			node.Stablize()
		}
		logrus.Info("[end]Maintain end: ", node.Addr)
	}()

	go func() {
		for node.Online {
			time.Sleep(MaintainTime)
			node.FixFinger()
		}
		logrus.Info("[end]Finger fix end: ", node.Addr)
	}()

	go func() {
		for node.Online {
			time.Sleep(MaintainTime)
			node.ChangePredecessor()
		}
		logrus.Info("[end]ChangePredecessor end: ", node.Addr)
	}()
}
func (node *Node) Stablize() error {

	var suc, pre NodeInformation
	node.RPCGetFirstSuccessor("", &suc)
	err := RemoteCall(suc.Addr, "Node.RPCGetPredecessor", "", &pre)
	if err != nil {
		logrus.Error("[error] Stabilize error: ", err)
		return err
	}

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

	return err
}

func (node *Node) FixFinger() error {

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
	return nil
}

func (node *Node) ChangePredecessor() error {
	if node.predecessor.Addr != "" && !node.Ping(node.predecessor.Addr) {
		node.predecessorLock.Lock()
		node.predecessor.Addr = ""
		node.predecessorLock.Unlock()
		node.backupLock.Lock()
		node.dataLock.Lock()
		for k, v := range node.backup {
			node.data[k] = v
		}
		node.dataLock.Unlock()
		node.backupLock.Unlock()
		var suc NodeInformation
		err := node.RPCGetFirstSuccessor("", &suc)
		if err != nil {
			return err
		}
		node.backupLock.RLock()
		RemoteCall(suc.Addr, "Node.BackupAdd", node.backup, nil)
		node.backupLock.RUnlock()
		node.backupLock.Lock()
		node.backup = make(map[string]([]Message))
		node.backupLock.Unlock()

	}
	return nil
}

func (node *Node) BackupAdd(backup map[string]([]Message), _ *struct{}) error {
	node.backupLock.Lock()
	for k, v := range backup {
		for _, value := range v {
			node.backup[k] = append(node.backup[k], value)
		}
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) RPCPing(_ string, _ *struct{}) error {
	return nil
}
func (node *Node) RPCSeed(_ string, res *string) error {
	*res = node.Seed
	return nil
}
func (node *Node) RPCNotify(addr NodeInformation, _ *struct{}) error {
	if node.predecessor.Addr == "" || in(node.predecessor.HashId, node.Addr.HashId, addr.HashId, false, false) {
		logrus.Info("[Lock]predecessorLock:", node.Addr)
		node.predecessorLock.Lock()
		node.predecessor.Addr = addr.Addr
		node.predecessor.HashId = gethash(node.predecessor.Addr)
		node.predecessorLock.Unlock()
		node.backupLock.Lock()
		err := RemoteCall(node.predecessor.Addr, "Node.SetBackup", node.Addr.Addr, &node.backup)
		node.backupLock.Unlock()
		if err != nil {
			return err
		}

	}
	return nil
}
func (node *Node) SetBackup(addr string, backup *(map[string]([]Message))) error {
	*backup = make(map[string]([]Message))
	node.dataLock.RLock()
	for k, v := range node.data {
		for _, value := range v {
			(*backup)[k] = append((*backup)[k], value)
		}
	}
	node.dataLock.RUnlock()
	return nil
}
func (node *Node) RPCGetFirstSuccessor(_ string, reply *NodeInformation) error {
	node.successorLock.RLock()
	defer node.successorLock.RUnlock()
	for i := 0; i < successorSize; i++ {

		if node.Ping(node.successorList[i].Addr) {
			(*reply).Addr = node.successorList[i].Addr
			(*reply).HashId = gethash(reply.Addr)

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
	(*reply).Addr = node.predecessor.Addr
	(*reply).HashId = gethash(reply.Addr)
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
		(*reply).Addr = suc.Addr
		(*reply).HashId = gethash(reply.Addr)
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
		(*reply).Addr = node.Addr.Addr
		(*reply).HashId = gethash(reply.Addr)
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
	(*reply).Addr = tar.Addr
	(*reply).HashId = gethash(reply.Addr)
	return nil
}
func (node *Node) RPCFindClosePrecedingFinger(addr NodeInformation, reply *NodeInformation) error {
	node.tableLock.RLock()
	defer node.tableLock.RUnlock()
	for i := hashSize - 1; i >= 0; i-- {
		if node.fingertable[i].Addr != "" && in(node.Addr.HashId, addr.HashId, node.fingertable[i].HashId, false, false) && node.Ping(node.fingertable[i].Addr) {
			(*reply).Addr = node.fingertable[i].Addr
			(*reply).HashId = gethash((*reply).Addr)
			return nil
		}
	}
	(*reply).Addr = node.Addr.Addr
	(*reply).HashId = gethash(reply.Addr)
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
	if !node.Ping(addr) {
		logrus.Error("[error] Join: addr shutdown", node.Addr.Addr, addr)
		return false
	}
	RemoteCall(addr, "Node.RPCSeed", "", &(node.Seed))
	var reply NodeInformation
	err := RemoteCall(addr, "Node.RPCFindSuccessor", node.Addr, &reply)
	if err != nil {
		logrus.Error("[error] Join: ", node.Addr.Addr, addr, err)
		return false
	}
	logrus.Info("[Success] Join: ", node.Addr.Addr, " ", reply.Addr, " ", addr)
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
	err = RemoteCall(reply.Addr, "Node.TransferData", node.Addr, &node.data)
	if err != nil {
		logrus.Error("[error] Transfer: ", node.Addr.Addr, " ", reply.Addr, " ", err)
		return false
	}
	node.Mantain()
	return true
}

func (node *Node) TransferData(pre NodeInformation, predata *(map[string]([]Message))) error {
	node.backupLock.Lock()
	node.backup = make(map[string]([]Message))
	node.dataLock.Lock()
	for k, v := range node.data {
		if !in(pre.HashId, node.Addr.HashId, gethash(k), false, true) {
			for _, value := range v {
				(*predata)[k] = append((*predata)[k], value)
				node.backup[k] = append(node.backup[k], value)
			}
		}
	}
	node.dataLock.Unlock()
	node.backupLock.Unlock()
	node.DataSub("", predata)
	var suc NodeInformation
	node.RPCGetFirstSuccessor("", &suc)
	err := RemoteCall(suc.Addr, "Node.BackupSub", "", predata)
	if err != nil {
		logrus.Error("[error] Transfer Backup")
	}
	node.predecessorLock.Lock()
	node.predecessor.Addr = pre.Addr
	node.predecessor.HashId = gethash(node.predecessor.Addr)
	node.predecessorLock.Unlock()
	return nil
}
func (node *Node) DataSub(_ string, data *(map[string]([]Message))) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	for k, _ := range *data {
		_, flag := node.data[k]
		if flag {
			delete(node.data, k)
		}
	}
	return nil
}
func (node *Node) BackupSub(_ string, data *(map[string]([]Message))) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	for k, _ := range *data {
		_, flag := node.backup[k]
		if flag {
			delete(node.backup, k)
		}
	}
	return nil
}
func (node *Node) Quit() {
	if !node.Online {
		return
	}
	node.quit <- true
	err := node.listener.Close()
	if err != nil {
		logrus.Error("[error] Quit error: ", node.Addr.Addr, err)
	}
	node.onlineLock.Lock()
	node.Online = false
	node.onlineLock.Unlock()
	logrus.Info("[Success] Quit: ", node.Addr.Addr)
	node.quit = make(chan bool, 1)
	logrus.Info("[Final] From: ", node.Addr.Addr, " Successor: ", node.successorList, " Predecessor: ", node.predecessor.Addr)
	logrus.Info("[Data] ", node.Addr.Addr, " ", node.data)
}
func (node *Node) ForceQuit() {
	if !node.Online {
		return
	}
	node.quit <- true
	err := node.listener.Close()
	if err != nil {
		logrus.Error("[error] Quit error: ", node.Addr.Addr, err)
	}
	node.onlineLock.Lock()
	node.Online = false
	node.onlineLock.Unlock()
	logrus.Info("[Success] Quit: ", node.Addr.Addr)
	node.quit = make(chan bool, 1)
}
func (node *Node) Put(key string, value Message) (check bool) {
	var suc NodeInformation
	err := node.RPCFindSuccessor(NodeInformation{"", gethash(key)}, &suc)
	if err != nil {
		logrus.Error("[error] Put in RPCFindSuccessor: ", node.Addr.Addr, " ", key, " ", value, " ", err)
		return false
	}
	err = RemoteCall(suc.Addr, "Node.Addindata", Pair{key, value}, nil)
	if err != nil {
		logrus.Error("[error] Put in Addindata: ", node.Addr.Addr, " ", key, " ", value, " ", err)
		return false
	}

	logrus.Info("[Success] Put in Addindata: ", suc.Addr, " ", key, " ", value)
	var sucsuc NodeInformation
	err = RemoteCall(suc.Addr, "Node.RPCGetFirstSuccessor", "", &sucsuc)
	if err != nil {
		logrus.Error("[error] Put in RPCGetFirstSuccessor: ", node.Addr.Addr, " ", key, " ", value, " ", err)
	} else {
		err = RemoteCall(sucsuc.Addr, "Node.Addinbackup", Pair{key, value}, nil)
		if err != nil {
			logrus.Warnln("[warn] Put in Addinbackup: ", node.Addr.Addr, " ", key, " ", value, " ", err)
		}
	}

	logrus.Info("[Success] Put in Addinbackup: ", sucsuc.Addr, " ", key, " ", value)
	return true
}
func (node *Node) Addindata(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[pair.Key] = append(node.data[pair.Key], pair.Value)
	node.dataLock.Unlock()

	return nil
}
func (node *Node) Addinbackup(pair Pair, _ *struct{}) error {
	node.backupLock.Lock()
	node.backup[pair.Key] = append(node.backup[pair.Key], pair.Value)
	node.backupLock.Unlock()

	return nil
}
func (node *Node) Get(key string) (check bool, value []Message) {
	var suc NodeInformation
	node.RPCFindSuccessor(NodeInformation{"", gethash(key)}, &suc)
	var ret []Message
	err := RemoteCall(suc.Addr, "Node.GetValue", key, &ret)
	if err != nil {
		check = false
		value = nil

	} else {
		check = true
		value = ret
	}
	return
}
func (node *Node) GetValue(key string, value *([]Message)) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	v, flag := node.data[key]
	if flag {
		*value = v
		return nil
	} else {
		*value = nil
		return fmt.Errorf("[error] GetValue: %v %v", node.Addr.Addr, key)
	}
}
func (node *Node) Delete(key string) (check bool) {
	var suc NodeInformation
	err := node.RPCFindSuccessor(NodeInformation{"", gethash(key)}, &suc)

	if err != nil {
		logrus.Error("[error] Del in RPCFindSuccessor: ", node.Addr.Addr, " ", key, " ", err)
		return false
	}
	err = RemoteCall(suc.Addr, "Node.Delvalueindata", key, nil)
	if err != nil {
		logrus.Error("[error] Del in Delvalueindata: ", node.Addr.Addr, " ", key, " ", err)
		return false
	}

	var sucsuc NodeInformation
	err = RemoteCall(suc.Addr, "Node.RPCGetFirstSuccessor", "", &sucsuc)
	if err != nil {
		logrus.Error("[error] Del in RPCGetFirstSuccessor: ", suc.Addr, " ", key, " ", err)
	} else {
		err = RemoteCall(sucsuc.Addr, "Node.Delvalueinback", key, nil)
		if err != nil {
			logrus.Warnln("[warn] Del in Delvalueinback: ", sucsuc.Addr, " ", key, " ", err)
		}
	}

	logrus.Info("[Success] Del: ", node.Addr.Addr, " ", key)
	return true
}
func (node *Node) Delvalueindata(key string, _ *struct{}) error {

	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	_, flag := node.data[key]
	if flag {
		delete(node.data, key)
		return nil
	} else {
		return fmt.Errorf("[error] Can't find key in data: %v %v", key, node.Addr.Addr)
	}
}

func (node *Node) Delvalueinback(key string, _ *struct{}) error {

	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	_, flag := node.backup[key]
	if flag {
		delete(node.backup, key)
		return nil
	} else {
		return fmt.Errorf("[error] Can't find key in backup: %v %v", key, node.Addr.Addr)
	}
}
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
