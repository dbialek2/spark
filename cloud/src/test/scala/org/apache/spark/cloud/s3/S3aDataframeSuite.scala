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
import org.apache.spark.cloud.s3.examples.S3DataFrames

/**
 * Test the [S3DataFrames] logic
 */
private[cloud] class S3aDataframeSuite extends CloudSuite with S3aTestSetup {

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

  ctest("S3ADataFrames",
    "S3A Data Frames",
    "Execute the S3A Data Frames example") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    val destDir = new Path(TestDir, "dataframes")
    val rowCount = 1000

    assert(0 === S3DataFrames.action(conf,
      Seq(destDir,
        rowCount))
    )

    // do a recursive listFiles
    val recursiveListResults = duration("listFiles(recursive)") {
      filesystem.listFiles(destDir, true)
    }

    val listing = toSeq(recursiveListResults)
    var recursivelyListedFilesDataset = 0L
    var recursivelyListedFiles = 0
    duration("scan result list") {
      listing.foreach{status =>
        recursivelyListedFiles += 1
        recursivelyListedFilesDataset += status.getLen
        logInfo(s"${status.getPath}[${status.getLen}]")
      }
    }

    logInfo(s"FileSystem $filesystem")
  }

}
