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

import org.apache.spark.SparkConf
import org.apache.spark.cloud.examples.ObjectStoreExample
import org.apache.spark.cloud.s3.S3AConstants

/**
 * Base Class for examples working with S3.
 */
private[cloud] trait S3ExampleSetup extends ObjectStoreExample {

  /**
   * Set the standard S3A Hadoop options to be used in test/examples
   * @param sparkConf spark configuration to patch
   */
  override protected def applyObjectStoreConfigurationOptions(sparkConf: SparkConf): Unit = {
    super.applyObjectStoreConfigurationOptions(sparkConf)
    // smaller block size to divide up work
    hconf(sparkConf, S3AConstants.BLOCK_SIZE, 1 * 1024 * 1024)
    hconf(sparkConf, S3AConstants.FAST_UPLOAD, "true")
    // have a smaller buffer for more writers
    hconf(sparkConf, S3AConstants.FAST_BUFFER_SIZE, 8192)
    // commit with v2 algorithm
    hconf(sparkConf, "mapreduce.fileoutputcommitter.algorithm.version", 2)
    hconf(sparkConf, "mapreduce.fileoutputcommitter.cleanup.skipped", "true")
  }
}
