package main

import (
	"flag"
	"math/rand"
	"os"
	"time"
)

var (
	help     bool
	testName string
)

func init() {
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&testName, "test", "", "which test(s) do you want to run: basic/advance/all")

	flag.Usage = usage
	flag.Parse()

	if help || (testName != "basic" && testName != "advance" && testName != "all") {
		flag.Usage()
		os.Exit(0)
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	yellow.Printf("Welcome to DHT-2023 Test Program!\n\n")

	var basicFailRate float64
	var forceQuitFailRate float64
	var QASFailRate float64

	switch testName {
	case "all":
		fallthrough
	case "basic":
		yellow.Println("Basic Test Begins:")
		basicPanicked, basicFailedCnt, basicTotalCnt := basicTest()
		if basicPanicked {
			red.Printf("Basic Test Panicked.")
			os.Exit(0)
		}

		basicFailRate = float64(basicFailedCnt) / float64(basicTotalCnt)
		if basicFailRate > basicTestMaxFailRate {
			red.Printf("Basic test failed with fail rate %.4f\n\n", basicFailRate)
		} else {
			green.Printf("Basic test passed with fail rate %.4f\n\n", basicFailRate)
		}

		if testName == "basic" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "advance":
		yellow.Println("Advance Test Begins:")

		/* ------ Force Quit Test Begins ------ */
		forceQuitPanicked, forceQuitFailedCnt, forceQuitTotalCnt := forceQuitTest()
		if forceQuitPanicked {
			red.Printf("Force Quit Test Panicked.")
			os.Exit(0)
		}

		forceQuitFailRate = float64(forceQuitFailedCnt) / float64(forceQuitTotalCnt)
		if forceQuitFailRate > forceQuitMaxFailRate {
			red.Printf("Force quit test failed with fail rate %.4f\n\n", forceQuitFailRate)
		} else {
			green.Printf("Force quit test passed with fail rate %.4f\n\n", forceQuitFailRate)
		}
		time.Sleep(afterTestSleepTime)
		/* ------ Force Quit Test Ends ------ */

		/* ------ Quit & Stabilize Test Begins ------ */
		QASPanicked, QASFailedCnt, QASTotalCnt := quitAndStabilizeTest()
		if QASPanicked {
			red.Printf("Quit & Stabilize Test Panicked.")
			os.Exit(0)
		}

		QASFailRate = float64(QASFailedCnt) / float64(QASTotalCnt)
		if QASFailRate > QASMaxFailRate {
			red.Printf("Quit & Stabilize test failed with fail rate %.4f\n\n", QASFailRate)
		} else {
			green.Printf("Quit & Stabilize test passed with fail rate %.4f\n\n", QASFailRate)
		}
		/* ------ Quit & Stabilize Test Ends ------ */
	}

	cyan.Println("\nFinal print:")
	if basicFailRate > basicTestMaxFailRate {
		red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
	} else {
		green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
	}
	if forceQuitFailRate > forceQuitMaxFailRate {
		red.Printf("Force quit test failed with fail rate %.4f\n", forceQuitFailRate)
	} else {
		green.Printf("Force quit test passed with fail rate %.4f\n", forceQuitFailRate)
	}
	if QASFailRate > QASMaxFailRate {
		red.Printf("Quit & Stabilize test failed with fail rate %.4f\n", QASFailRate)
	} else {
		green.Printf("Quit & Stabilize test passed with fail rate %.4f\n", QASFailRate)
	}
}

func usage() {
	flag.PrintDefaults()
}

// package main

// import (
// 	"fmt"
// 	"math/rand"
// 	"time"
// )

// func main() {
// 	rand.Seed(time.Now().Unix())
// 	length := 100
// 	nodes := make([]dhtNode, length, length)
// 	nodeAddresses := make([]string, length, length)
// 	for i := 0; i < length; i++ {
// 		nodes[i] = NewNode(8800 + i)
// 		nodeAddresses[i] = portToAddr(localAddress, 8800+i)
// 		go nodes[i].Run()
// 	}
// 	nodesInNetwork := make([]int, 0, length)
// 	time.Sleep(time.Second)
// 	nodes[0].Create()
// 	nodesInNetwork = append(nodesInNetwork, 0)
// 	nextJoinNode := 1
// 	time.Sleep(time.Second)
// 	for i := 1; i < length; i++ {
// 		addr := nodeAddresses[nodesInNetwork[rand.Intn(len(nodesInNetwork))]]
// 		if !nodes[nextJoinNode].Join(addr) {
// 			fmt.Println("failed")
// 		} else {
// 			fmt.Println("Success")
// 		}
// 		nodesInNetwork = append(nodesInNetwork, nextJoinNode)
// 		nextJoinNode++
// 		time.Sleep(time.Second * 2)
// 	}
// 	time.Sleep(time.Second * 20)
// 	go nodes[0].Quit()
// 	go nodes[10].Quit()
// 	go nodes[20].Quit()
// 	go nodes[30].Quit()
// 	go nodes[40].Quit()
// 	go nodes[50].Quit()
// 	go nodes[60].Quit()
// 	go nodes[70].Quit()
// 	go nodes[80].Quit()
// 	go nodes[90].Quit()
// 	time.Sleep(time.Second * 20)
// 	for i := 0; i < length; i++ {
// 		flag := nodes[i].Check()
// 		fmt.Println("Check ", i, " ", flag)
// 	}
// 	for i := 0; i < length; i++ {
// 		go nodes[i].Quit()
// 	}

// 	time.Sleep(time.Second * 5)
// }

// func main() {
// 	rand.Seed(time.Now().Unix())
// 	length := 10
// 	nodes := make([]dhtNode, length, length)
// 	nodeAddresses := make([]string, length, length)
// 	for i := 0; i < length; i++ {
// 		nodes[i] = NewNode(8800 + i)
// 		nodeAddresses[i] = portToAddr(localAddress, 8800+i)
// 		go nodes[i].Run()
// 	}
// 	nodesInNetwork := make([]int, 0, length)
// 	time.Sleep(time.Second)
// 	nodes[0].Create()
// 	nodesInNetwork = append(nodesInNetwork, 0)
// 	nextJoinNode := 1
// 	time.Sleep(time.Second)
// 	for i := 1; i < length; i++ {
// 		addr := nodeAddresses[nodesInNetwork[rand.Intn(len(nodesInNetwork))]]
// 		if !nodes[nextJoinNode].Join(addr) {
// 			fmt.Println("failed")
// 		} else {
// 			fmt.Println("Success")
// 		}
// 		nodesInNetwork = append(nodesInNetwork, nextJoinNode)
// 		nextJoinNode++
// 		time.Sleep(time.Second * 2)
// 	}
// 	time.Sleep(time.Second * 10)
// 	nodes[0].Put("1", "jpp")
// 	nodes[1].Put("2", "jpp")
// 	nodes[2].Put("3", "jpp")
// 	nodes[3].Put("4", "jpp")
// 	nodes[4].Put("5", "jpp")
// 	nodes[5].Put("6", "jpp")
// 	nodes[6].Put("7", "jpp")
// 	nodes[7].Put("8", "jpp")
// 	nodes[0].Quit()
// 	time.Sleep(time.Second * 2)
// 	nodes[1].Quit()
// 	time.Sleep(time.Second * 2)
// 	nodes[2].Quit()
// 	time.Sleep(time.Second * 2)
// 	nodes[3].Quit()
// 	time.Sleep(time.Second * 10)
// 	fmt.Println(nodes[9].Get("1"))
// 	fmt.Println(nodes[9].Get("2"))
// 	fmt.Println(nodes[9].Get("3"))
// 	fmt.Println(nodes[9].Get("4"))
// 	fmt.Println(nodes[9].Get("5"))
// 	fmt.Println(nodes[9].Get("6"))
// 	fmt.Println(nodes[9].Get("7"))
// 	fmt.Println(nodes[9].Get("8"))
// 	time.Sleep(time.Second * 2)
// 	// nodes[0].Delete("1")
// 	// nodes[1].Delete("2")
// 	// nodes[2].Delete("3")
// 	// nodes[3].Delete("4")
// 	// nodes[4].Delete("5")
// 	// nodes[5].Delete("6")
// 	// nodes[6].Delete("7")
// 	// nodes[7].Delete("8")
// 	// time.Sleep(time.Second * 10)
// 	// fmt.Println(nodes[9].Get("1"))
// 	// fmt.Println(nodes[9].Get("2"))
// 	// fmt.Println(nodes[9].Get("3"))
// 	// fmt.Println(nodes[9].Get("4"))
// 	// fmt.Println(nodes[9].Get("5"))
// 	// fmt.Println(nodes[9].Get("6"))
// 	// fmt.Println(nodes[9].Get("7"))
// 	// fmt.Println(nodes[9].Get("8"))
// 	for i := 0; i < length; i++ {
// 		flag := nodes[i].Check()
// 		fmt.Println("Check ", i, " ", flag)
// 	}
// 	for i := 0; i < length; i++ {
// 		go nodes[i].Quit()
// 	}
// 	time.Sleep(time.Second * 5)
// }
