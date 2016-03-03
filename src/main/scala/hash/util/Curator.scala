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

import java.nio.ByteBuffer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException._
import org.apache.zookeeper.Watcher
import scala.collection.JavaConversions._


final case class ZooKeeper(host: String = "localhost", port: Int = 2181) {

  require(0 < port, "Invalid port value: "+port)

  override def toString(): String = host + ":" + port

}

trait Curator {

  def client: CuratorFramework

  def create[T](znode: String, mode: CreateMode = EPHEMERAL)

  def set(znode: String, value: Array[Byte])

  def get(znode: String): Option[Array[Byte]]

  def exists(znode: String): Boolean 

  def list(znode: String): Map[String, Array[Byte]]

  def list(znode: String, watcher: Watcher): Map[String, Array[Byte]]

}

object Curator {

  def create(targets: Seq[ZooKeeper]): Curator = {
    val servers = targets.map(_.toString).mkString(",")
    val client = CuratorFrameworkFactory.builder.
                                         sessionTimeoutMs(3*60*1000).
                                         retryPolicy(new RetryNTimes(3, 1000)).
                                         connectString(servers).build
    client.start
    new DefaultCurator(client)
  } 

  private def error(msg: String) = throw new IllegalArgumentException(msg)

}

class DefaultCurator(framework: CuratorFramework) extends Curator {

  require(null != framework, "Curator framework instance is missing!")

  import Curator._

  override def client: CuratorFramework = framework

  override def create[T](znode: String, mode: CreateMode = EPHEMERAL): Unit = 
    znode match {
      case null | "" => error("Invalid znode!")
      case p if !p.startsWith("/") => error("Invalid znode: "+p)
      case _ => framework.create.creatingParentsIfNeeded.withMode(mode).
                          forPath(znode)
    }

  override def set(znode: String, value: Array[Byte]) = 
    framework.setData.forPath(znode, value)  

  override def get(znode: String): Option[Array[Byte]] = 
    Option(framework.getData.forPath(znode))

  override def exists(znode: String): Boolean = 
    framework.checkExists.forPath(znode) match {
      case stat: Stat => true
      case _ => false
    }

  override def list(znode: String): Map[String, Array[Byte]] = 
    framework.getChildren.forPath(znode).toSeq.map { h =>
      (h, framework.getData.forPath(znode+"/"+h)) 
    }.toMap

  override def list(znode: String, watcher: Watcher): Map[String, Array[Byte]]= 
    framework.getChildren.usingWatcher(watcher).forPath(znode).toSeq.map { h =>
      (h, framework.getData.forPath(znode+"/"+h)) 
    }.toMap

}
