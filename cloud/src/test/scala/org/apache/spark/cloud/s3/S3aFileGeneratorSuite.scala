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

package org.apache.spark.cloud.s3

import org.apache.hadoop.fs.Path

import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.s3.examples.S3FileGenerator

/**
 * Test the `S3FileGenerator` entry point.
 */
private[cloud] class S3aFileGeneratorSuite extends CloudSuite with S3aTestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  after {
    cleanFilesystemInTeardown()
  }

  ctest("FileGeneratorUsage",
    "S3A File Generator",
    "Execute the S3FileGenerator example with a bad argument; expect a failure") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    assert(-2 === S3FileGenerator.action(conf, Array()))
  }

  ctest("FileGenerator",
    "S3A File Generator",
    "Execute the S3FileGenerator example") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    val destDir = new Path(TestDir, "filegenerator")
    val fileCount = 10
    val rowCount = 10000
    assert(0 === S3FileGenerator.action(conf,
      Array(destDir.toString,
        fileCount.toString,
        rowCount.toString)))
    val status = filesystem.getFileStatus(destDir)
    assert(status.isDirectory, s"Not a directory: $status")
    val files = filesystem.listStatus(destDir,
      pathFilter(p => p.getName != "_SUCCESS"))
    var listStatusSize = 0L
    var filenames = ""
    val listStatusFileCount = 0
    files.foreach { f =>
      listStatusSize += f.getLen
      filenames = filenames + " " + f.getPath.getName
    }
    logInfo(s"total size = $listStatusSize bytes from ${files.length} files: $filenames")
    assert(listStatusSize > rowCount,
      s"Total file length ($listStatusSize) less than expected $rowCount")

    logInfo(s"FileSystem $filesystem")
    // now do a recursive listFiles
    val recursiveListResults = duration("listFiles(recursive)") {
      filesystem.listFiles(destDir, true)
    }
    logInfo(s"FileSystem $filesystem")

    val listing = duration("results to sequence") {
      toSeq(recursiveListResults)
    }
    logInfo(s"FileSystem $filesystem")
    var list2Size = 0L
    var list2FileCount = 0
    duration("scan result list") {
      listing.foreach{s =>
        list2FileCount += 1
        list2Size += s.getLen
      }
    }

    logInfo(s"FileSystem $filesystem")
    assert(fileCount === list2FileCount)
    assert(listStatusSize === list2Size)


  }

}
