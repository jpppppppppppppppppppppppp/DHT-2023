package main

import (
	"dht/src"
)

/*
 * In this file, you need to implement the "NewNode" function.
 * This function should create a new DHT node and return it.
 * You can use the "naive.Node" struct as a reference to implement your own struct.
 */

func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	node := new(src.Node)
	node.Init(portToAddr(localAddress, port))
	return node
}
