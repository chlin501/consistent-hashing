/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hash

import java.nio.charset.Charset
import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import hash.util.ZooKeeper

class ConsistentHashingSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(classOf[ConsistentHashingSpec])

  var zookeeper: Option[TestingServer] = None

  val nodeA = Node(port = 1000, replicas = 2)
  val nodeB = Node(port = 2000, replicas = 2)
  val nodeC = Node(port = 3000, replicas = 2)

  override def beforeAll { zookeeper match {
    case Some(zk) =>
    case None => zookeeper = Option(new TestingServer())
  }}

  "consistent hashing" should "post nodes to zookeeper" in {
    val map = ConsistentHashing.create(ZooKeeper(port = getPort)).
                      post(nodeA, nodeB, nodeC).list
    log.info("map in zookeeper "+map)
    assert(6 == map.size)
  }

  "consistent hashing" should "detect node created" in {
    val hashing1 = ConsistentHashing.create("/ring1", ZooKeeper(port = getPort))
    val hashing2 = ConsistentHashing.create("/ring1", ZooKeeper(port = getPort))
    val map1 = hashing1.post(Node(port = 1000, replicas = 2)).list
    log.info("initialize post (hashing1): "+map1)
    assert(2 == map1.size)
    val map2 = hashing2.post(Node(port = 2000, replicas = 2)).list
    log.info("second post (hashing2): "+map2)
    assert(4 == map2.size)
    val altered = hashing1.list
    log.info("hashing1 map (altered): "+altered)
    assert(4 == altered.size)
  }

  "consistent hashing" should "assign object to node" in {
    val hashing = ConsistentHashing.create(ZooKeeper(port = getPort)).
                          post(nodeA, nodeB, nodeC)
    val node1 = hashing.findBy(new Hashable() {
      override def hash(): String = toHash("john")
    })
    log.info("john is assigned to "+node1)
    assert(Option(nodeA).equals(node1))
    val node2 = hashing.findBy(new Hashable() {
      override def hash(): String = toHash("smith")
    })
    log.info("smith is assigned to "+node2)
    assert(Option(nodeB).equals(node2))
    val node3 = hashing.findBy(new Hashable() {
      override def hash(): String = toHash("brown")
    })
    log.info("brown is assigned to "+node3)
    assert(Option(nodeC).equals(node3))
    val node4 = hashing.findBy(new Hashable() {
      override def hash(): String = toHash("zoe")
    })
    log.info("zoe is assigned to "+node4)
    assert(Option(nodeA).equals(node4))
    val node5 = hashing.findBy(new Hashable() {
      override def hash(): String = toHash("adam")
    })
    log.info("adam is assigned to "+node5)
    assert(Option(nodeB).equals(node5))
  }

  private def toHash(input: String): String = com.google.common.hash.Hashing.
    sipHash24.hashString(input, Charset.forName("UTF-8")).toString

  private def getPort(): Int = zookeeper.map { zk => 
    zk.getPort 
  }.getOrElse(2181)


  override def afterAll { zookeeper match {
    case Some(zk) => zk.close
    case None => 
  }}

}

