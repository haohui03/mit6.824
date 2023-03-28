# zookeeper

## why zookeeper
- provide general-purpose coordinate service(unlike raft-a library)

## Guarantees
- linearizable writes
- FIFO Client order


## paper-reading: **ZooKeeper: Wait-free coordination for Internet-scale systems**

### Abstract 

- In addition to the wait-free
property, ZooKeeper provides a per client guarantee of FIFO execution of requests *(FIFO for one client)* and linearizability for all requests that change the ZooKeeper state *(writes are linearizable)*.

### Intro

#### Key-point is coordination
- Configuration
- Group membership and leader election 

>When designing our coordination service, we moved away from implementing specific primitives on the server side, and instead we opted for exposing an API that enables application developers to implement their own primitives.

>When designing the API of ZooKeeper, we moved away from blocking primitives, such as locks.
















### link

[Zookeeper v.s Chubby](https://zhuanlan.zhihu.com/p/30991749)