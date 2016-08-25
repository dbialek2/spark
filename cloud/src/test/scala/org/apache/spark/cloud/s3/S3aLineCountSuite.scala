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

import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.s3.examples.S3LineCount

/**
 * Test the `S3LineCount` entry point.
 */
private[cloud] class S3aLineCountSuite extends CloudSuite with S3aTestSetup {

  init()

  override def useCSVEndpoint: Boolean = true

  def init(): Unit = {
    setupFilesystemConfiguration(conf)
  }

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  ctest("S3ALineCountReadData",
    "S3A Line count default values",
    "Execute the S3ALineCount example with the default values (i.e. no arguments)") {
    val sparkConf = newSparkConf(CSV_TESTFILE.get)
    sparkConf.setAppName("S3ALineCountDefaults")
    assert(0 === S3LineCount.action(sparkConf, Array()))
  }

}
