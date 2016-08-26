/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.cloud.utils

import scala.reflect.ClassTag

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem, Path, PathFilter, RemoteIterator}
import org.apache.hadoop.io.{NullWritable, Text}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
 * Extra Hadoop operations for object store integration.
 */
private[cloud] trait ObjectStoreOperations extends Logging {

  /**
   * Save this RDD as a text file, using string representations of elements.
   *
   * There's a bit of convoluted-ness here, as this supports writing to any Hadoop FS,
   * rather than the default one in the configuration ... this is addressed by creating a
   * new configuration
   */
  def saveAsTextFile[T](rdd: RDD[T], path: Path, conf: Configuration): Unit = {
    rdd.withScope {
      val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
      val textClassTag = implicitly[ClassTag[Text]]
      val r = rdd.mapPartitions { iter =>
        val text = new Text()
        iter.map { x =>
          text.set(x.toString)
          (NullWritable.get(), text)
        }
      }
      val pathFS = FileSystem.get(path.toUri, conf)
      val confWithTargetFS = new Configuration(conf)
      confWithTargetFS.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        pathFS.getUri.toString)
      val pairOps = RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      pairOps.saveAsNewAPIHadoopFile(path.toUri.toString,
        pairOps.keyClass, pairOps.valueClass,
        classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[NullWritable, Text]],
        confWithTargetFS)
    }
  }

  /**
   * Take a predicate, generate a path filter from it.
   * @param filterPredicate predicate
   * @return a filter which uses the predicate to decide whether to accept a file or not
   */
  def pathFilter(filterPredicate: Path => Boolean): PathFilter = {
    new PathFilter {
      def accept(path: Path): Boolean = filterPredicate(path)
    }
  }

  /**
   * Take the output of a remote iterator and covert it to a scala sequence. Network
   * IO may take place during the operation, and changes to a remote FS may result in
   * a sequence which is not consistent with any single state of the FS.
   * @param source source
   * @tparam T type of source
   * @return a sequence
   */
  def toSeq[T](source: RemoteIterator[T]): Seq[T] = {
    new RemoteOutputIterator[T](source).toSeq
  }

  /**
   * Put a string to the destination
   * @param path path
   * @param conf configuration to use when requesting the filesystem
   * @param body string body
   */
  def put(path: Path, conf: Configuration, body: String): Unit = {
    val fs = FileSystem.get(path.toUri, conf)
    val out = fs.create(path, true)
    try {
      IOUtils.write(body, out)
    } finally {
      IOUtils.closeQuietly(out)
    }
  }

  /**
   * Spin-wait for a predicate to evaluate to true, sleeping between probes
   * and raising an exception if the condition is not met before the timeout.
   * @param timeout time to wait
   * @param interval sleep interval
   * @param message exception message
   * @param predicate predicate to evaluate
   */
  def await(timeout: Long, interval: Int = 500,
      message: => String = "timeout")(predicate: => Boolean): Unit = {
    val endTime = now() + timeout
    var succeeded = false;
    while (!succeeded && now() < endTime) {
      succeeded = predicate
      if (!succeeded) {
        Thread.sleep(interval)
      }
    }
    if (!succeeded) {
      throw new Exception(message)
    }
  }

  def now(): Long = {
    System.currentTimeMillis()
  }
}

/**
 * Iterator over remote output.
 * @param source source iterator
 * @tparam T type of response
 */
class RemoteOutputIterator[T](private val source: RemoteIterator[T]) extends Iterator[T] {
  def hasNext: Boolean = source.hasNext

  def next: T = source.next()
}
