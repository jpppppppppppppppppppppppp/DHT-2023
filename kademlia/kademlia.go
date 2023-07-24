package kademlia

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
	PingwaitTime       = 10 * time.Second
	Republishtime      = 120 * time.Second
	Expiretime         = 960 * time.Second
	RepublishSleep     = 100 * time.Millisecond
	ExpireSleep        = 100 * time.Millisecond
	hashSize       int = 160
	K              int = 45
	alpha          int = 5
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

func distance(x, y *big.Int) big.Int {
	var ret big.Int
	ret.Xor(x, y)
	return ret
}

func cpl(x, y *big.Int) int {
	dis := distance(x, y)
	return dis.BitLen() - 1
}

type Pair struct {
	Key   string
	Value string
}

type NodeInformation struct {
	Addr   string
	HashId big.Int
}

type dataset struct {
	Value         map[string]string
	Republishtime map[string]time.Time
	Expiretime    map[string]time.Time
	mux           sync.RWMutex
}
type Node struct {
	Addr       NodeInformation
	online     bool
	onlineLock sync.RWMutex
	listener   net.Listener
	server     *rpc.Server
	routeTable KBucket
	quit       chan bool
	data       dataset
}
type List struct {
	Size int
	Data [K]string
	mux  sync.RWMutex
}

func (list *List) Init() {
	list.Size = 0
}

type KBucket struct {
	bucket [hashSize]List
}

func (node *Node) Init(add string) {
	node.Addr.Addr = add
	node.Addr.HashId = *gethash(add)
	for i := 0; i < hashSize; i++ {
		node.routeTable.bucket[i].Init()
	}
	node.quit = make(chan bool, 1)
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.data.mux.Lock()
	node.data.Value = make(map[string]string)
	node.data.Expiretime = make(map[string]time.Time)
	node.data.Republishtime = make(map[string]time.Time)
	node.data.mux.Unlock()
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
		// logrus.Error("[error] Dialing error: ", addr, err)
		return client, err
	}
	client = rpc.NewClient(conn)
	return client, err
}

func RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	client, err := GetClient(addr)
	if err != nil {
		// logrus.Error("[error] GetClient error: ", addr, err)
		return err
	}
	defer client.Close()
	err = client.Call(method, args, reply)
	// if err != nil {
	// 	logrus.Error("[error] Call error: ", addr, method, args, reply, err)
	// } else {
	// 	logrus.Infoln("[Success] Call: ", addr, method)
	// }
	return err
}
func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		// logrus.Error("[error] Register error: ", node.Addr.Addr, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.Addr.Addr)
	if err != nil {
		// logrus.Error("[error] TCP listener error: ", node.Addr.Addr, err)
		return
	}
	node.onlineLock.Lock()
	node.online = true
	node.onlineLock.Unlock()
	go func() {
		for node.online {
			select {
			case <-node.quit:
				return
			default:
				conn, err := node.listener.Accept()
				if err != nil {
					// logrus.Error("[error] Accept error: ", err)
					// logrus.Info("[end] Run end: ", node.Addr)
					return
				}
				go node.server.ServeConn(conn)
			}
		}
		// logrus.Info("[end] Run end: ", node.Addr)
	}()
	node.Maintain()
	// logrus.Infoln("[Success] Run: ", node.Addr.Addr)
}
func (node *Node) Maintain() {
	go func() {
		for node.online {
			node.Republish()
			time.Sleep(RepublishSleep)
		}
	}()
	go func() {
		for node.online {
			node.Expire()
			time.Sleep(ExpireSleep)
		}
	}()
}
func (node *Node) Republish() {
	node.data.mux.RLock()
	republishlist := make(map[string]string)
	for key, t := range node.data.Republishtime {
		if time.Now().After(t) {
			republishlist[key] = node.data.Value[key]
		}
	}
	node.data.mux.RUnlock()
	// logrus.Info("[BeginRepublish] ", node.Addr.Addr, " ", republishlist)
	for key, value := range republishlist {
		// logrus.Info("[Republish] ", node.Addr.Addr, " ", key)

		node.Put(key, value)
	}
}
func (node *Node) Expire() {
	var expirelist []string
	node.data.mux.Lock()
	for key, t := range node.data.Expiretime {
		if time.Now().After(t) {
			expirelist = append(expirelist, key)
		}
	}
	for _, key := range expirelist {
		// logrus.Info("[Expire] ", node.Addr.Addr, " ", key)
		delete(node.data.Expiretime, key)
		delete(node.data.Republishtime, key)
		delete(node.data.Value, key)
		// logrus.Info("[Expire] ", node.Addr.Addr, key)
	}
	node.data.mux.Unlock()
}
func (node *Node) Create() {
	return
}
func (node *Node) RPCPing(_ string, _ *struct{}) error {
	return nil
}
func (node *Node) Join(addr string) bool {
	if !node.Ping(addr) {
		return false
	}
	node.Update(addr)
	node.FindClosestKNode(node.Addr.Addr)
	return true
}
func (node *Node) Update(addr string) {
	if addr == "" || node.Addr.Addr == addr {
		return
	}
	ind := cpl(&node.Addr.HashId, gethash(addr))
	pos := -1
	node.routeTable.bucket[ind].mux.Lock()
	defer node.routeTable.bucket[ind].mux.Unlock()
	for i := 0; i < node.routeTable.bucket[ind].Size; i++ {
		if node.routeTable.bucket[ind].Data[i] == addr {
			pos = i
			break
		}
	}
	if pos == -1 {
		if node.routeTable.bucket[ind].Size < K {
			node.routeTable.bucket[ind].Data[node.routeTable.bucket[ind].Size] = addr
			node.routeTable.bucket[ind].Size++
			return
		} else {
			if node.Ping(node.routeTable.bucket[ind].Data[0]) {
				head := node.routeTable.bucket[ind].Data[0]
				for i := 1; i < K; i++ {
					node.routeTable.bucket[ind].Data[i-1] = node.routeTable.bucket[ind].Data[i]
				}
				node.routeTable.bucket[ind].Data[K-1] = head
				return
			} else {
				for i := 1; i < K; i++ {
					node.routeTable.bucket[ind].Data[i-1] = node.routeTable.bucket[ind].Data[i]
				}
				node.routeTable.bucket[ind].Data[K-1] = addr
				return
			}
		}
	} else {
		for i := pos + 1; i < K; i++ {
			node.routeTable.bucket[ind].Data[i-1] = node.routeTable.bucket[ind].Data[i]
		}
		node.routeTable.bucket[ind].Data[node.routeTable.bucket[ind].Size-1] = addr
	}
}

type OrderedNodeList struct {
	target   string
	NodeList List
}

func (olist *OrderedNodeList) Insert(addr string) bool {
	pos := -1
	for i := 0; i < olist.NodeList.Size; i++ {
		if olist.NodeList.Data[i] == addr {
			pos = i
			break
		}
	}

	if pos != -1 {
		return false
	}
	dis := distance(gethash(olist.target), gethash(addr))
	if olist.NodeList.Size < K {
		for i := 0; i < olist.NodeList.Size; i++ {
			olddis := distance(gethash(olist.target), gethash(olist.NodeList.Data[i]))
			if dis.Cmp(&olddis) == -1 {
				for j := olist.NodeList.Size; j > i; j-- {
					olist.NodeList.Data[j] = olist.NodeList.Data[j-1]
				}
				olist.NodeList.Data[i] = addr
				olist.NodeList.Size++
				return true
			}
		}
		olist.NodeList.Data[olist.NodeList.Size] = addr
		olist.NodeList.Size++
		return true
	} else {
		for i := 0; i < K; i++ {
			olddis := distance(gethash(olist.target), gethash(olist.NodeList.Data[i]))
			if dis.Cmp(&olddis) == -1 {
				for j := K - 1; j > i; j-- {
					olist.NodeList.Data[j] = olist.NodeList.Data[j-1]
				}
				olist.NodeList.Data[i] = addr
				return true
			}
		}
	}
	return false
}

func (olist *OrderedNodeList) Remove(addr string) {
	for i := 0; i < olist.NodeList.Size; i++ {
		if addr == olist.NodeList.Data[i] {
			for j := i + 1; j < olist.NodeList.Size; j++ {
				olist.NodeList.Data[j-1] = olist.NodeList.Data[j]
			}
			olist.NodeList.Size--
			return
		}
	}
}

func (node *Node) FindNode(addr string, res *OrderedNodeList) error {
	// logrus.Info("[Find] ", addr, " ", node.Addr.Addr)
	res.target = addr
	for i := 0; i < hashSize; i++ {
		node.routeTable.bucket[i].mux.RLock()
		for j := 0; j < node.routeTable.bucket[i].Size; j++ {
			res.Insert(node.routeTable.bucket[i].Data[j])
		}
		node.routeTable.bucket[i].mux.RUnlock()
	}
	return nil
}
func (node *Node) RPCFindNode(args FindNodeargs, res *OrderedNodeList) error {
	err := node.FindNode(args.Target, res)
	node.Update(args.From)
	return err
}
func (node *Node) FindValue(key string, res *Findvaluereply) error {

	node.data.mux.RLock()
	value, check := node.data.Value[key]
	node.data.mux.RUnlock()
	if check {
		res.Value = value
		// logrus.Info("[Get] ", key, " ", node.Addr.Addr, " ", res)
		return nil
	}
	res.Value = ""
	res.Nodelists.target = key
	for i := 0; i < hashSize; i++ {
		node.routeTable.bucket[i].mux.RLock()
		for j := 0; j < node.routeTable.bucket[i].Size; j++ {
			res.Nodelists.Insert(node.routeTable.bucket[i].Data[j])
		}
		node.routeTable.bucket[i].mux.RUnlock()
	}
	// logrus.Info("[Get] ", key, " ", node.Addr.Addr, " ", res)
	return nil
}
func (node *Node) RPCFindValue(args FindNodeargs, res *Findvaluereply) error {
	err := node.FindValue(args.Target, res)
	node.Update(args.From)
	return err
}

type Findvaluereply struct {
	Value     string
	Nodelists OrderedNodeList
}
type FindNodeargs struct {
	From   string
	Target string
}

func (node *Node) FindClosestKNode(addr string) OrderedNodeList {
	// logrus.Info("[FindK] ", node.Addr.Addr, " ", addr)
	var res OrderedNodeList
	node.FindNode(addr, &res)
	res.Insert(node.Addr.Addr)
	updated := true
	vis := make(map[string]bool)
	for updated {
		// logrus.Info("[FindPre] ", addr, " ", res)
		updated = false
		var newlist OrderedNodeList
		var removed []string
		for i := 0; i < res.NodeList.Size; i++ {
			if i == alpha {
				break
			}
			if vis[res.NodeList.Data[i]] {
				continue
			}
			vis[res.NodeList.Data[i]] = true
			node.Update(res.NodeList.Data[i])
			var reply OrderedNodeList
			err := RemoteCall(res.NodeList.Data[i], "Node.RPCFindNode", FindNodeargs{node.Addr.Addr, addr}, &reply)
			if err != nil {
				removed = append(removed, res.NodeList.Data[i])
			} else {
				for j := 0; j < reply.NodeList.Size; j++ {
					if node.Ping(reply.NodeList.Data[j]) {
						newlist.Insert(reply.NodeList.Data[j])
						// logrus.Info("[FindNew] ", flag, " ", addr, " ", res.NodeList.Data, " ", newlist.NodeList.Data)
					}
				}
			}
		}
		for _, what := range removed {
			res.Remove(what)
			updated = true
		}
		for i := 0; i < newlist.NodeList.Size; i++ {
			flag := res.Insert(newlist.NodeList.Data[i])
			// logrus.Info("[FindUp] ", flag, " ", addr, " ", res.NodeList.Data, " ", newlist.NodeList.Data[i])
			updated = updated || flag
		}
	}
	return res
}
func (node *Node) Quit() {
	if !node.online {
		return
	}
	node.listener.Close()
	// if err != nil {
	// 	logrus.Error("[error] Quit error: ", node.Addr.Addr, err)
	// }
	node.quit <- true
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quit = make(chan bool, 1)
	// logrus.Info("[Success] Quit: ", node.Addr.Addr)
}
func (node *Node) ForceQuit() {
	if !node.online {
		return
	}
	node.listener.Close()
	// if err != nil {
	// 	logrus.Error("[error] Quit error: ", node.Addr.Addr, err)
	// }
	node.quit <- true
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quit = make(chan bool, 1)
	// logrus.Info("[Success] Quit: ", node.Addr.Addr)
}
func (node *Node) Ping(addr string) bool {
	err := RemoteCall(addr, "Node.RPCPing", node.Addr.Addr, nil)
	if err != nil {
		return false
	} else {
		return true
	}
}
func (node *Node) Put(key string, value string) bool {
	res := node.FindClosestKNode(key)
	// logrus.Info("[Put] ", key, " ", res.NodeList)
	res.Insert(node.Addr.Addr)
	for i := 0; i < res.NodeList.Size; i++ {
		RemoteCall(res.NodeList.Data[i], "Node.Store", Storeargs{key, value, node.Addr.Addr}, nil)
		// if err != nil {
		// 	logrus.Warnln("[Warn] in put", flag)
		// }
	}
	return true
}

type Storeargs struct {
	Key   string
	Value string
	Addr  string
}

func (node *Node) Store(args Storeargs, _ *struct{}) error {
	// logrus.Info("[Store] ", node.Addr.Addr, " ", args)
	node.Update(args.Addr)
	node.data.mux.Lock()
	defer node.data.mux.Unlock()
	node.data.Value[args.Key] = args.Value
	node.data.Republishtime[args.Key] = time.Now().Add(Republishtime)
	node.data.Expiretime[args.Key] = time.Now().Add(Expiretime)
	return nil
}
func (node *Node) Get(key string) (bool, string) {
	// logrus.Info("[GGet] ", key, " ", node.Addr.Addr)
	vis := make(map[string]bool)
	var res Findvaluereply
	node.FindValue(key, &res)
	if res.Value != "" {
		// logrus.Info("[V] ", true, " ", key, " ", res.Value)
		return true, res.Value
	}
	updated := true
	for updated {
		// logrus.Info("[GetPre] ", key, " ", res)
		updated = false
		var newlist OrderedNodeList
		var removed []string
		for i := 0; i < res.Nodelists.NodeList.Size; i++ {
			if vis[res.Nodelists.NodeList.Data[i]] {
				continue
			}
			vis[res.Nodelists.NodeList.Data[i]] = true
			var reply Findvaluereply
			err := RemoteCall(res.Nodelists.NodeList.Data[i], "Node.RPCFindValue", FindNodeargs{node.Addr.Addr, key}, &reply)
			if err != nil {
				removed = append(removed, res.Nodelists.NodeList.Data[i])
			} else {
				if reply.Value != "" {
					// logrus.Info("[V] ", true, " ", key, " ", res.Value)
					return true, reply.Value
				}
				for j := 0; j < reply.Nodelists.NodeList.Size; j++ {
					if node.Ping(reply.Nodelists.NodeList.Data[j]) {
						newlist.Insert(reply.Nodelists.NodeList.Data[j])
						// logrus.Info("[GetNew] ", flag, " ", key, " ", res.Nodelists.NodeList.Data, " ", newlist.NodeList.Data)
					}
				}
			}
		}
		for _, what := range removed {
			res.Nodelists.Remove(what)
			updated = true
		}
		for i := 0; i < newlist.NodeList.Size; i++ {
			flag := res.Nodelists.Insert(newlist.NodeList.Data[i])
			// logrus.Info("[update] ", flag, " ", key, " ", res, " ", newlist.NodeList.Data[i])
			updated = updated || flag
		}
	}
	// logrus.Info("[^] ", false, " ", key, " ", res.Value)
	return false, ""
}
func (node *Node) Delete(key string) bool { return true }
func (node *Node) Check() {
	fmt.Print(node.Addr.Addr, " ")
	for i := 0; i < hashSize; i++ {
		fmt.Print(node.routeTable.bucket[i].Size, " ")
	}
	fmt.Println(node.data.Value)
}
