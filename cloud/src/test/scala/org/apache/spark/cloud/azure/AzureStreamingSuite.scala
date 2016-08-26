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

package org.apache.spark.cloud.azure

import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.examples.CloudStreaming

/**
 * Test Streaming.
 */
private[cloud] class AzureStreamingSuite extends CloudSuite with AzureTestSetup {

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

  ctest("AzureStreaming", "Execute the Azure Streaming example") {
    val conf = newSparkConf()
    conf.setAppName("Streaming")
    val destDir = testPath(filesystem, "streaming")
    val rowCount = 1000

    assert(0 === new CloudStreaming().action(conf, Seq(destDir, rowCount)))
  }

}
