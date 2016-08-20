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

/**
 * Populate a directory tree
 */
object S3DirectoryPopulator extends S3ExampleBase {
  /**
   * Default action: returns 0
   * @param sparkConf configuration to use
   * @param args argument array; the first argument must be the destination filename.
   * @return an exit code
   */
  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {
    0
  }
}
