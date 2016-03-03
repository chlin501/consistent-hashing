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

import com.google.common.hash.Funnel
import com.google.common.hash.Hashing
import com.google.common.hash.PrimitiveSink
import hash.util.Curator
import hash.util.ZooKeeper
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.charset.Charset
import org.apache.zookeeper.KeeperException._
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.Watcher.Event.EventType._
import org.apache.zookeeper.WatchedEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

object Node {

  def toBytes(node: Node): Array[Byte] = {
    val bout = new ByteArrayOutputStream
    val dout = new DataOutputStream(bout)
    dout.writeUTF(node.host) 
    dout.writeInt(node.port)
    dout.writeInt(node.replicas)
    val bytes = bout.toByteArray
    dout.close
    bytes
  }

  def from(bytes: Array[Byte]): Node = {
    val bin = new ByteArrayInputStream(bytes)
    val din = new DataInputStream(bin)
    val host = din.readUTF
    val port = din.readInt
    val replicas = din.readInt
    Node(host, port, replicas)
  }

}

final case class Node(host: String = "localhost", port: Int = 1234, 
                      replicas: Int = 100) {

  def toMap(): Map[Long, Node] = (for(idx <- 1 to replicas) yield {
    (Hashing.sipHash24.hashObject(this, new Funnel[Node]() {
      override def funnel(from: Node, into: PrimitiveSink) =  
        into.putUnencodedChars(from.host).putInt(from.port).putInt(idx)
    }).toString.toLong -> this)
  }).toMap[Long, Node]

}

trait ConsistentHashing {

  def post(node: Node*): ConsistentHashing

  def list: Map[Long, Node]

  def findBy(key: String): Option[Node]

  def client: Curator

}

object ConsistentHashing {
  
  val root = "/ring" // TODO: from conf
  
  def create(zookeepers: ZooKeeper*): ConsistentHashing = 
    new DefaultConsistentHashing(Curator.create(zookeepers.toSeq))

}

protected[hash] class DefaultConsistentHashing(curator: Curator) 
      extends ConsistentHashing {
 
  import ConsistentHashing._

  val log = LoggerFactory.getLogger(classOf[DefaultConsistentHashing])

  protected[hash] var ring = SortedMap.empty[Long, Node]

  protected[hash] val defaultWatcher = new DefaultWatcher(this)

  protected[hash] class DefaultWatcher(main: DefaultConsistentHashing) 
      extends Watcher {

    override def process(event: WatchedEvent): Unit = event.getType match {
      case NodeCreated => {
        val hash = event.getPath
        //log.info("znode found: "+hash)
        val bytes = main.client.get(hash).getOrElse(Node.toBytes(Node()))
        ring += (hash.toLong -> Node.from(bytes))
        main.client.list(root, main.defaultWatcher)  
      }
      case ChildWatchRemoved =>
      case DataWatchRemoved => 
      case NodeChildrenChanged => 
      case NodeDataChanged => 
      case NodeDeleted =>
      case None =>
    }
  }

  initialize()

  override def client: Curator = curator

  protected[hash] def initialize() = curator.list(root, defaultWatcher)

  protected[hash] def retry(times: Int, path: String, f: String => Unit) {
    if(0 < times) {
      if(!curator.exists(path)) try {
        f(path)
      } catch {
        case n: NodeExistsException => log.warn("Path "+path+" already exists!")
        case e : Exception => log.error("Unable to create znode: "+path, e)
      }
    }
  }

  override def post(nodes: Node*): ConsistentHashing = {
    ring ++= nodes.map { node => node.toMap }.flatten.toMap
    retry(5, root, { p => curator.create(root) })
    ring.foreach { case (hash, node) => 
      val znode = root+"/"+hash 
      if(!curator.exists(znode)) curator.set(znode, Node.toBytes(node)) 
    }
    this
  } 

  def list: Map[Long, Node] = curator.list(root).map { case (hash, bytes) => 
    (hash.toLong, Node.from(bytes)) 
  }.toMap

  def findBy(key: String): Option[Node] = TreeMap(list.toArray:_*).
    from(hash(key)).headOption.orElse(ring.headOption).map(_._2)

  protected def hash(input: String): Long = 
    Hashing.sipHash24.hashString(input, Charset.forName("UTF-8")).toString.toLong

}
