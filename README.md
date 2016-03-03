### Introduction

  This is a demo for consistent hashing with zookeeper.

### Prerequisite 

  * java version 8
  * scala version 2.11.7
  * sbt version 0.13.9
  
### Usage

  ```scala
  val hashing = ConsistenHashing.create(ZooKeeper(host="127.0.0.1", port=1234))
  // post a node to zookeeper with 2 replicas
  hashing.post(Node(port = 1000, replicas = 2)) 
  // list mapping in zookeeper
  hashing.list
  // find mapped node 
  val node = hashing.findBy("smith")
  ``` 
  
