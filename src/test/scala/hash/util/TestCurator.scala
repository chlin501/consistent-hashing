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
package hash.util

import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CuratorSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val log = LoggerFactory.getLogger(classOf[CuratorSpec])
  var zookeeper: Option[TestingServer] = None

  before { zookeeper match {
    case Some(zk) =>
    case None => zookeeper = Option(new TestingServer(2181))
  }}

  "curator" should "create stats" in {
    val curator = Curator.create(Seq(ZooKeeper()))
    curator.create("/ring/hash1")
    curator.create("/ring/hash2")
    val exists1 = curator.exists("/ring/hash1")
    val exists2 = curator.exists("/ring/hash2")
    assert(exists1 && exists2)
  }

  after { zookeeper match {
    case Some(zk) => zk.close
    case None => 
  }}

}

