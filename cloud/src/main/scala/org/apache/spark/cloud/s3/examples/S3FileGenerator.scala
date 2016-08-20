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

package org.apache.spark.cloud.s3.examples

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Generate a file containing some numbers in the remote repository.
 */
object S3FileGenerator extends S3ExampleBase {

  private val USAGE = "Usage S3FileGenerator <filename> <file-count> <row-count>"
  private val DEFAULT_COUNT: Integer = 1000

  /**
   * Generate a file containing some numbers in the remote repository.
   * @param sparkConf configuration to use
   * @param args argument array; the first argument must be the destination filename.
   * @return an exit code
   */
  override def action(sparkConf: SparkConf, args: Array[String]): Int = {
    val l = args.length
    if (l != 3) {
      // wrong number of arguments
      return usage()
    }
    val dest = args(0)
    val fileCount = intArg(args, 1, 0)
    val rowCount = intArg(args, 2, 0)
    val destURI = new URI(dest)
    val destPath = new Path(destURI)
    logInfo(s"Dest file = $destURI; count=$rowCount")
    // smaller block size to divide up work
    hconf(sparkConf, "fs.s3a.block.size", (1 * 1024 * 1024).toString)
    // commit with v2 algorithm
    hconf(sparkConf, "mapreduce.fileoutputcommitter.algorithm.version", "2")
    val sc = new SparkContext(sparkConf)
    def eval[T](rdd: RDD[T]) = {
      sc.runJob(rdd, (it: Iterator[T]) => {
        while (it.hasNext) it.next()
      })
    }
    try {
      val destFs = FileSystem.get(destURI, sc.hadoopConfiguration)
      // create the parent directories or fail
      destFs.delete(destPath, true)
      destFs.mkdirs(destPath.getParent())
      val destPathSer = destPath.toUri
      val filesRDD = sc.parallelize(1 to fileCount)
      val filepathRDD = filesRDD.map(entry =>
        new Path(new Path(destPathSer), "job-%04s".format(entry)).toUri
      )
      // now build them
      val fileset = duration(s"save $rowCount values") {
        filesRDD.map(entry => {
          val dP = new Path(destPathSer)
          val jobDest = new Path(destPath, "job-%04s".format(entry))
          val numbers = sc.parallelize(1 to rowCount)
          numbers.saveAsTextFile(destPathSer.toString)
          jobDest
        })
      }

      val status = destFs.getFileStatus(destPath)
      logInfo(s"Generated file $status")
      logInfo(s"File System = $destFs")
      // read it back
      val input = sc.textFile(dest)
      val c2 = duration(s" count $status") {
        input.count()
      }
      logInfo(s"Read value = $c2")
    } finally {
      logInfo("Stopping Spark Context")
      sc.stop()
    }
    0
  }

  def usage(): Int = {
    logError(USAGE)
    EXIT_USAGE
  }
}


