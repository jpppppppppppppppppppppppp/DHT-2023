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
// 	"sync"
// 	"time"
// )

// func main() {
// 	nodes := new([20]dhtNode)
// 	nodeAddresses := new([20]string)

// 	/* "Run" all nodes. */
// 	wg = new(sync.WaitGroup)
// 	for i := 0; i < 20; i++ {
// 		nodes[i] = NewNode(firstPort + i)
// 		nodeAddresses[i] = portToAddr(localAddress, firstPort+i)

// 		wg.Add(1)
// 		go nodes[i].Run()
// 	}
// 	nodes[0].Create()
// 	time.Sleep(basicTestJoinQuitSleepTime)
// 	for i := 1; i < 20; i++ {
// 		nodes[i].Join(nodeAddresses[0])
// 		time.Sleep(basicTestJoinQuitSleepTime)
// 	}
// 	nodes[0].Put("jpp", "dsg")
// 	time.Sleep(basicTestJoinQuitSleepTime)
// 	for i := 0; i < 20; i++ {
// 		nodes[i].Check()
// 	}
// 	time.Sleep(basicTestJoinQuitSleepTime)
// 	for i := 0; i < 20; i++ {
// 		nodes[i].Quit()
// 	}
// 	time.Sleep(basicTestJoinQuitSleepTime * 5)
// }
