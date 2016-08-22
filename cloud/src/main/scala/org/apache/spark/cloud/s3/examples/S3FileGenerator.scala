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

import org.apache.spark.cloud.utils.ConfigSerDeser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Generate a file containing some numbers in the remote repository.
 */
object S3FileGenerator extends S3ExampleBase {

  private val USAGE = "Usage S3FileGenerator <dest> <start-year> <years> <file-count> <row-count>"

  /**
   * Generate a file containing some numbers in the remote repository.
   * @param sparkConf configuration to use
   * @param args argument array; the first argument must be the destination filename.
   * @return an exit code
   */
  override def action(sparkConf: SparkConf, args: Array[String]): Int = {
    val l = args.length
    if (l != 5) {
      // wrong number of arguments
      return usage()
    }
    val dest = args(0)
    val startYear = intArg(args, 1, 2016)
    val yearCount = intArg(args, 2, 1)
    val fileCount = intArg(args, 3, 1)
    val rowCount = intArg(args, 4, 0)
    val destURI = new URI(dest)
    val destPath = new Path(destURI)
    logInfo(s"Dest file = $destURI; count=$rowCount")
    // smaller block size to divide up work
    hconf(sparkConf, "fs.s3a.block.size", (1 * 1024 * 1024).toString)
    // commit with v2 algorithm
    hconf(sparkConf, "mapreduce.fileoutputcommitter.algorithm.version", "2")
    hconf(sparkConf, "mapreduce.fileoutputcommitter.cleanup.skipped", "true")

    val sc = new SparkContext(sparkConf)
    try {
      val years = startYear until startYear + yearCount
      val months = 1 to 12
      // list of (YYYY, 1), (YYYY, 2), ...
      val monthsByYear = years.flatMap(year =>
        months.map(m => (year, m))
      )
      val filePerMonthRange = 1 to fileCount
      // build paths like 2016/2016-05/2016-05-0012.txt
      val filepaths = monthsByYear.flatMap { case (y, m) =>
        filePerMonthRange.map(r =>
          "%1$04d/%1$04d-%2$02d/%1$04d-%2$02d-%3$04d.txt".format(y, m, r)
        )
      }.map(new Path(destPath, _))
      val fileURIs = filepaths.map(_.toUri)

      val builder = new StringBuilder(rowCount * 6)
      for (i <- 1 to rowCount) yield {
        builder.append(i).append("\n")
      }
      val body = builder.toString()

      val destFs = FileSystem.get(destURI, sc.hadoopConfiguration)
      // create the parent directories or fail
      destFs.delete(destPath, true)
      destFs.mkdirs(destPath.getParent())
      val configSerDeser = new ConfigSerDeser(sc.hadoopConfiguration)
      // RDD to save the text to every path in the files RDD, returning path and
      // the time it took
      val filesRDD = sc.parallelize(fileURIs)
      val putDataRDD = filesRDD.map(uri => {
        val jobDest = new Path(uri)
        val hc = configSerDeser.get()
        val executionTime = time(put(jobDest, hc, body))
        (jobDest.toUri, executionTime)
      })
      logInfo(s"Initial File System state = $destFs")

      // Trigger the evaluations of the RDDs
      val executionResults = duration("result collection") {
        putDataRDD.collect()
      }
      logInfo(s"File System = $destFs")

      // now list all files under the path
      val (listing, started, listDuration) = duration2(destFs.listFiles(destPath, true))
      logInfo(s"time to list paths under $destPath: $listDuration")

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


