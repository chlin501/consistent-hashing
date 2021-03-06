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
name := "consistent-hashing"

organization := "consistent-hashing"

version := "0.1"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "Apache" at "https://repository.apache.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "com.google.code.findbugs" % "jsr305" % "3.0.1",
  "com.google.guava" % "guava" % "19.0",
  "org.apache.curator" % "curator-framework" % "3.1.0",
  "org.apache.curator" % "curator-recipes" % "3.1.0",
  "org.apache.curator" % "curator-test" % "3.1.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.12",
  "junit" % "junit" % "4.10",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

