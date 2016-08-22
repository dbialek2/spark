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
import org.apache.spark.sql.SparkSession

/**
 * Create ORC File dataset
 */
object S3OrcFiles extends S3ExampleBase {

  def action(): Unit = {

  }

  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {
    applyS3AConfigOptions(sparkConf)

    val dest = args(0)
    val rowCount = intArg(args, 1, 1000)

    val session = SparkSession
        .builder
        .appName("S3OrcFiles")
        .config(sparkConf)

        .getOrCreate()

    import session.implicits._

    session.
    try {

    } finally {
      session.stop()
    }

    0
  }

}
