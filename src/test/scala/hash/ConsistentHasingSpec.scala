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

import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import hash.util.ZooKeeper

class ConsistentHashingSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val log = LoggerFactory.getLogger(classOf[ConsistentHashingSpec])

  var zookeeper: Option[TestingServer] = None

  before { zookeeper match {
    case Some(zk) =>
    case None => zookeeper = Option(new TestingServer())
  }}

  "consistent hashing" should "post nodes to zookeeper" in {
    val p = zookeeper.map { zk => zk.getPort }.getOrElse(2181)
    val hashing = ConsistentHashing.create(ZooKeeper(port = p))
    val map = hashing.post (
      Node(port = 1000, replicas = 2), 
      Node(port = 2000, replicas = 2),
      Node(port = 3000, replicas = 2)
    ).list
    log.info("map in zookeeper "+map)
    assert(6 == map.size)
  }

  after { zookeeper match {
    case Some(zk) => zk.close
    case None => 
  }}

}

