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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.cloud.s3.S3AConstants._

/**
 * A line count example which has a default reference of a public Amazon S3
 * CSV .gz file in the absence of anything on the command line.
 */
object S3LineCount extends S3ExampleSetup {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    "[<source>] [<dest>]"
  }

  /**
   * Count all lines in a file in a remote object store.
   * This is scoped to be accessible for testing.
   *
   * If there is no destination file, the configuration is patched to allow for S3A
   * anonymous access on Hadoop 2.8+.
   *
   * The option to set the credential provider is not supported on Hadoop 2.6/2.7,
   * so the spark/cluster configuration must contain any credentials needed to
   * authenticate with AWS.
   * @param sparkConf configuration to use
   * @param args argument array; if empty then the default CSV path is used.
   * @return an exit code
   */
  override def action(sparkConf: SparkConf, args: Array[String]): Int = {
    if (args.length > 2) {
      return usage()
    }
    val source = arg(args, 0, S3A_CSV_PATH_DEFAULT)
    val dest = arg(args, 1)
    logInfo(s"Source file = $source")
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)
    val _1MB = 1024 * 1024
    // smaller block size to divide up work
    applyObjectStoreConfigurationOptions(sparkConf)
    val blockSize = _1MB
    hconf(sparkConf, BLOCK_SIZE, blockSize)
    hconf(sparkConf, FAST_UPLOAD, "true")
    hconf(sparkConf, MULTIPART_SIZE, MIN_PERMITTED_MULTIPART_SIZE)
    hconf(sparkConf, MIN_MULTIPART_THRESHOLD, MIN_PERMITTED_MULTIPART_SIZE)
    // hadoop 2.7.x will fail if trying to set the multipart purge policy on a read-only FS.
    hconf(sparkConf, PURGE_EXISTING_MULTIPART, "false")

    // If there is no destination, switch to the anonymous provider.
    if (dest.isEmpty) {
      hconf(sparkConf, AWS_CREDENTIALS_PROVIDER, ANONYMOUS_CREDENTIALS)
    }
    logInfo(s"Data Source $srcURI")

    val sc = new SparkContext(sparkConf)
    try {
      val sourceFs = FileSystem.newInstance(srcURI, sc.hadoopConfiguration)

      // this will throw an exception if the source file is missing
      val status = sourceFs.getFileStatus(srcPath)
      logInfo(s"Source details: $status")
      if (status.getBlockSize != blockSize) {
        logWarning(s"Blocksize wrong: expected $blockSize  but got ${status.getBlockSize}")
      }
      val input = sc.textFile(source)
      if (dest.isEmpty) {
        // no destination: just do a count
        val count = duration(s" count $srcPath") {
          input.count()
        }
        logInfo(s"line count = $count")
        logInfo(s"File System = $sourceFs")
      } else {
        // destination provided
        val destUri = new URI(dest.get)
        logInfo(s"Destination $destUri")
        val destFs = FileSystem.get(destUri, sc.hadoopConfiguration)
        duration("save") {
          val destPath = new Path(destUri)
          destFs.delete(destPath, true)
          destFs.mkdirs(destPath.getParent())
          saveAsTextFile(input, destPath, sc.hadoopConfiguration)
          val status = destFs.getFileStatus(destPath)
          logInfo(s"Output Dir $status")
          logInfo(s"File System = $destFs")
        }
      }

    } finally {
      logInfo("Stopping Spark Context")
      sc.stop()
    }
    0
  }

}
