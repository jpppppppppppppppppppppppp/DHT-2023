# Distributed Hash Table - PPCA 2023  Report

## Some Detials
* When trying to quit a node, in order to maintain the it's successor's data and backup, just need to make successor's predecessor equal to "" and add all the k-v pairs in backup into it's dataset. By the predecessor's stablizing to notify its successor to back it up.

* When fixing bugs, please confirm if it is reason for the time delay, and then print as much debugging information as possible.

## Chord
### 文件结构

* src/pp.go:定义了和Chord算法相关的所有实现

* hash/hash.go:哈希部分
