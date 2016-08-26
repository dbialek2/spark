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

package org.apache.spark.cloud.examples

import scala.collection.mutable.StringBuilder

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
 * An example/test for streaming with a source of cloud infra
 */
private[cloud] class CloudStreaming extends ObjectStoreExample {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    "<dest> [<rows>]"
  }

  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {
    if (args.length < 1 || args.length > 2) {
      return usage()
    }
    sparkConf.setAppName("CloudStreaming")
    applyObjectStoreConfigurationOptions(sparkConf)

    // Create the context
    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))

    try {
      // Create the FileInputDStream on the directory regexp and use the
      // stream to look for a new file renamed into it
      val destDir = new Path(args(0))
      val streamDir = new Path(destDir, "streaming")
      val streamGlobPath = new Path(streamDir, "sub*")
      val generatedDir = new Path(destDir, "generated");
      val generatedSubDir = new Path(generatedDir, "subdir");
      val renamedSubDir = new Path(streamDir, "subdir");

      val sparkContext = ssc.sparkContext
      val hc = sparkContext.hadoopConfiguration

      val fs = FileSystem.get(destDir.toUri, hc)
      fs.delete(destDir, true)
      fs.mkdirs(destDir)
      fs.mkdirs(streamDir)
      val sightings = sparkContext.longAccumulator("sightings")

      logInfo(s"Looking for text files under $streamGlobPath")
      val lines = ssc.textFileStream(streamGlobPath.toUri.toString)
      val rowCount = 100
      val builder = new StringBuilder(rowCount * 6)
      for (i <- 1 to rowCount) yield {
        builder.append(i).append("\n")
      }
      val body = builder.toString

      val matches = lines.filter(_.endsWith("3")).map(line => {
        sightings.add(1)
        line
      })

      matches.print()
      ssc.start()

      Thread.sleep(2500)
      // put a file into the generated directory
      val textPath = new Path(generatedSubDir, "body1.txt")
      duration(s"upload $textPath") {
        put(textPath, hc, body)
      }
      duration(s"rename $generatedSubDir to $renamedSubDir") {
        fs.rename(generatedSubDir, renamedSubDir)
      }
      val expected = rowCount / 10
      await(10000, 500, s"Expected $expected matches, saw ${sightings.value}") {
        sightings.value == expected
      }
      logInfo(s"FileSystem local stats: $fs")
      0
    } finally {
      ssc.stop(true)
    }
  }

}

private[cloud] object CloudStreaming {

  def main(args: Array[String]) {
    new CloudStreaming().main(args)
  }
}
