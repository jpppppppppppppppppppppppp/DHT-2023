# Distributed Hash Table - PPCA 2023

## Overview

A DHT is a distributed system that provides a lookup service similar to a hash table: (key, value) pairs are stored in the DHT, and any participating node can efficiently retrieve the value associated with a given key.
The goal of a DHT is to store and retrieve data in a scalable, efficient and reliable manner.

There are many algorithms to implement DHT. For this project, you are required to **implement Chord protocol and Kademlia protocol**. You should **write a report for about one page**, probably about your architecture, innovation, features and references. As a bonus, you can also implement an application of DHT.

## Tutorial

First, you should read the [Environment Setup](doc/env-setup.md) to setup your environment.

A naive implementation of `dhtNode` is provided in `naive/node.go`. You can use it as a reference. The code is well commented. It is suggested to **read it carefully**.

You can read the [Tutorial](doc/tutorial.md) for more information about Go, DHT and how to debug.

## Scores

- 40% for the Chord Test
  - 30% Basic test: naive test without "force quit".
  - 10% Advance test: "Force quit" will be tested. There will be some more complex tests.
- 40% for the Kademlia Test (Same as above)
- 20% for a short report and code review
- Extra 10% for the application of DHT

## Tests

Note: **DHT tests cannot run successfully under Windows or WSL 1**. See [Environment Setup](doc/env-setup.md) for more information.

Contact TA if you find any bug in the test program, or if you have some test ideas, or if you think the tests are too hard and you want TA to make it easier.

### Basic Test

There are **5 rounds** of test in total. In each round,

1. **20 nodes** join the network. Then **sleep for 10 seconds.**
2. **Put 150 key-value pairs**, **query for 120 pairs**, and then **delete 75 pairs**. There is **no sleep time between two contiguous operations**.
3. **10 nodes** quit from the network. Then **sleep for 10 seconds**.
4. (The same as 2.) **Put 150 key-value pairs**, **query for 120 pairs**, and then **delete 75 pairs**. There is **no sleep time between two contiguous operations**.

### Advance Test

The advance test consists of "**Force-Quit Test**" and "**Quit & Stabilize Test**".

#### Force-Quit Test

The current test procedure is:

* In the beginning, **50 nodes** join the network.
* Then **put 500 key-value pairs**.
* It follows by **9 rounds** of force quit. In each round,
  1. **5 nodes force-quit** from the network. There is **500ms of sleep time** between each force-quit operation.
  2. **Query for all key-value pairs**.

#### Quit & Stabilize Test

The current test procedure is:

* In the beginning, **50 nodes** join the network.
* Then **put 500 key-value pairs**.
* Next, **every node will quit from the network**:
  1. One node quits.
  2. After the node quitting from the network, there is **80ms of sleep time**. And then **20 key-value pairs will be queried for**.

## Some Detials
* When trying to quit a node, in order to maintain the it's successor's data and backup, just need to make successor's predecessor equal to "" and add all the k-v pairs in backup into it's dataset. By the predecessor's stablizing to notify its successor to back it up.

* When fixing bugs, please confirm if it is reason for the time delay, and then print as much debugging information as possible.