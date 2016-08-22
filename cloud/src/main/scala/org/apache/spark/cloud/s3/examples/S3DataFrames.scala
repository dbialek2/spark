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


import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.WriterContainer
import org.apache.spark.sql.types.StringType

/**
 * Simple dataframe operations
 */
object S3DataFrames extends S3ExampleBase {

  case class Data(l: Long, s: String)

  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {
    applyS3AConfigOptions(sparkConf)

    val dest = new Path(args(0))
    val rowCount = intArg(args, 1, 1000)
    // compile time check that the spark-sql code is on the classpath
    val forceBinding= WriterContainer.DATASOURCE_WRITEJOBUUID

    val spark = SparkSession
        .builder
        .appName("S3OrcFiles")
        .config(sparkConf)

        .getOrCreate()

    import spark.implicits._
    val numRows = 1000
    // simple benchmark code from DataSetBenchmark

    try {
      val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
      val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))

      def write(format: String) = {
        val p = new Path(dest, format)
        df.write.format(format).save(p.toString)
        p
      }
      val orc = write("orc")
      val parquet = write("parquet")
      val json = write("json")
      val csv = write("csv")

    } finally {
      spark.stop()
    }

    0
  }

}
