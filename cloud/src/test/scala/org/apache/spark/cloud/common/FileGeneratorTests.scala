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

package org.apache.spark.cloud.common

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.examples.CloudFileGenerator

/**
 * Test the `FileGenerator` entry point.
 */
private[cloud] abstract class FileGeneratorTests extends CloudSuite {

  ctest("FileGenerator", "Execute the FileGenerator example") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    val destDir = testPath(filesystem, "filegenerator")
    val startYear = 2015
    val yearCount = 1
    val fileCount = 2
    val rowCount = 1000

    assert(0 === generate(conf, destDir, startYear, yearCount, fileCount, rowCount))

    val status = filesystem.getFileStatus(destDir)
    assert(status.isDirectory, s"Not a directory: $status")

    val totalExpectedFiles = yearCount * 12 * fileCount

    // do a recursive listFiles
    val recursiveListResults = duration("listFiles(recursive)") {
      filesystem.listFiles(destDir, true)
    }

    val listing = toSeq(recursiveListResults)
    var recursivelyListedFilesDataset = 0L
    var recursivelyListedFiles = 0
    duration("scan result list") {
      listing.foreach { status =>
        recursivelyListedFiles += 1
        recursivelyListedFilesDataset += status.getLen
        logInfo(s"${status.getPath}[${status.getLen}]")
      }
    }

    logInfo(s"FileSystem $filesystem")
    assert(totalExpectedFiles === recursivelyListedFiles)
  }

  def generate(conf: SparkConf, destDir: Path,
      startYear: Int, yearCount: Int, fileCount: Int, rowCount: Int): Int = {
    val result = new CloudFileGenerator().action(conf, Seq(destDir,
      startYear,
      yearCount,
      fileCount,
      rowCount))
    result
  }
}
