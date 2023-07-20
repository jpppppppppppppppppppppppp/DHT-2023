package main

type dhtNode interface {
	// "Run" is called after calling "NewNode". You can do some initialization works here.
	Run()

	// "Create" or "Join" will be called after calling "Run".
	// For a dhtNode, either "Create" or "Join" will be called, but not both.

	// Create a new network.
	Create()
	// Join an existing network. Return "true" if join succeeded and "false" if not.
	Join(addr string) bool

	// "Normally" quit from current network.
	// You can inform other nodes in the network that you are leaving.
	// "Quit" will not be called before "Create" or "Join".
	// For a dhtNode, "Quit" may be called for many times.
	// For a quited node, call "Quit" again should have no effect.
	Quit()

	// Quit the network without informing other nodes.
	// "ForceQuit" will be checked by TA manually.
	ForceQuit()
	Check()
	// Check whether the node identified by addr is in the network.
	Ping(addr string) bool
	// Put a key-value pair into the network (if key exists, update the value).
	// Return "true" if success, "false" otherwise.
	Put(key string, value string) bool
	// Get a key-value pair from the network.
	// Return "true" and the value if success, "false" otherwise.
	Get(key string) (bool, string)
	// Remove a key-value pair identified by KEY from the network.
	// Return "true" if success, "false" otherwise.
	Delete(key string) bool
}
