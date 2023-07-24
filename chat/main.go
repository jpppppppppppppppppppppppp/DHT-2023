package main

import (
	"bufio"
	"chat/dht"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type UserNode struct {
	dhtNode  dht.Node
	username string
}

func input(reader *bufio.Reader) string {
	input, _ := reader.ReadString('\n')
	input = strings.Replace(input, "\n", "", -1)
	return input
}
func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("%c[1;40;33mEnter your username:\t%c[0m", 0x1B, 0x1B)
	Username := input(reader)
	fmt.Printf("%c[1;40;33mEnter IP Address to Bind (xxx.xxx.xxx.xxx:yyyy):\t%c[0m", 0x1B, 0x1B)
	Bindaddress := input(reader)
	fmt.Printf("%c[1;40;33mEnter IP Address to Name (xxx.xxx.xxx.xxx:yyyy):\t%c[0m", 0x1B, 0x1B)
	Nameaddress := input(reader)
	user := new(UserNode)
	user.username = Username
	user.dhtNode.Init(Bindaddress, Nameaddress)
	fmt.Printf("%c[1;40;33mJoin or Create?[y/Y]\t%c[0m", 0x1B, 0x1B)
	choice := input(reader)
	if choice == "Y" || choice == "y" {
		fmt.Printf("%c[1;40;33mEnter your:hostaddr\t%c[0m", 0x1B, 0x1B)
		hostaddress := input(reader)
		user.dhtNode.Run()
		time.Sleep(300 * time.Millisecond)
		flag := user.dhtNode.Join(hostaddress)
		if !flag {
			fmt.Printf("%c[1;40;31m[Error] Not such hostaddress online\n", 0x1B)
			return
		}
		fmt.Println(user.dhtNode.Seed)
	} else {
		user.dhtNode.Run()
		time.Sleep(300 * time.Millisecond)
		user.dhtNode.Create()
		time.Sleep(300 * time.Millisecond)
		fmt.Println(user.dhtNode.Seed)
	}
	fmt.Printf("%c[3;40;32mConnecting to Peerchat", 0x1B)
	time.Sleep(300 * time.Millisecond)
	fmt.Printf(".")
	time.Sleep(300 * time.Millisecond)
	fmt.Printf(".")
	time.Sleep(300 * time.Millisecond)
	fmt.Printf(".%c[0m\n", 0x1B)
	for {
		fmt.Printf("\n\n %c[1;40;36m>>>\t%c[0m", 0x1B, 0x1B)
		text := input(reader)
		if text == "" {
			// do nothing
			user.paint()
		} else if text == "exit" {
			// exit peerchat
			fmt.Printf("%c[1;40;32mExiting Peerchat!\n", 0x1B)
			user.dhtNode.Quit()
			break

		} else {
			user.send(text)
			user.paint()
		}
	}

}
func (user *UserNode) paint() {
	key := user.dhtNode.Seed + time.Now().Format("2006-01-02 15")
	checker, message := user.dhtNode.Get(key)
	if checker {
		for i := 0; i < 120-len(message); i++ {
			fmt.Print("\n")
		}
		sort.Sort(MessageSet(message))
		for _, item := range message {
			if item.From == user.username {
				fmt.Printf("%c[1;40;32m<%s>\t%c[1;40;31m[%s]%c[0m\t%s\n", 0x1B, item.Timestamp.Format("2006-01-02 15:04:05"), 0x1B, item.From, 0x1B, item.Message)
			} else {
				fmt.Printf("%c[1;40;32m<%s>\t%c[1;40;36m[%s]%c[0m\t%s\n", 0x1B, item.Timestamp.Format("2006-01-02 15:04:05"), 0x1B, item.From, 0x1B, item.Message)
			}
		}
	}
}

type MessageSet []dht.Message

func (Set MessageSet) Len() int {
	return len(Set)
}
func (Set MessageSet) Swap(i, j int) {
	Set[i], Set[j] = Set[j], Set[i]
}
func (Set MessageSet) Less(i, j int) bool {
	return Set[i].Timestamp.Before(Set[j].Timestamp)
}
func (user *UserNode) send(message string) {
	key := user.dhtNode.Seed + time.Now().Format("2006-01-02 15")
	user.dhtNode.Put(key, dht.Message{Timestamp: time.Now(), Message: message, From: user.username})
}
