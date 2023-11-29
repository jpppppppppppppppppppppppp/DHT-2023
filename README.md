# Distributed Hash Table - PPCA 2023  Report

## Some Details in Debug
* When trying to quit a node, in order to maintain the it's successor's data and backup, just need to make successor's predecessor equal to "" and add all the k-v pairs in backup into it's dataset. By the predecessor's stablizing to notify its successor to back it up.

* When fixing bugs, please confirm if it is reason for the time delay, and then print as much debugging information as possible.

### 文件结构

* chord/chord.go			：定义了和Chord算法相关的所有实现
* kademlia/kademlia.go：定义了和Kademlia算法相关的所有实现
* chat/main.go				 ：有application的实现

## P2P Chat Help

首先查看WSL分配到的ip，每次重新启动WSL都会改变。

```
$ ip a
1: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP group default qlen 1000
    link/ether 00:15:5d:2f:29:7d brd ff:ff:ff:ff:ff:ff
    inet 172.20.141.225/20 brd 172.20.143.255 scope global eth0
       valid_lft forever preferred_lft forever
    inet6 fe80::215:5dff:fe2f:297d/64 scope link
       valid_lft forever preferred_lft forever
```

比如这里是`172.20.141.225`，是`Listener`Bind的地址，然后把你想使用的端口映射到真实网卡的端口上

```
netsh interface portproxy add v4tov4 listenport=8888 listenaddress=0.0.0.0 connectport=8888 connectaddress=Bindaddr
```

进入程序后依次输入自己的Bindaddr和Nameaddr即可。

加入已存在的网络需要任何一个已在网络中的Nameaddr。