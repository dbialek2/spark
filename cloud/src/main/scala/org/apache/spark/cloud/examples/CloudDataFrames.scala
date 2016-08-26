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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
 * Test dataframe operations using an object store as the destination and source of operations.
 * This validates the various conversion jobs all work against the object store.
 *
 * It doesn't verify timings, though some information is printed.
 */
private[cloud] class CloudDataFrames extends ObjectStoreExample {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    " <dest> [<rows>]"
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

    val dest = new Path(args(0))
    val rowCount = intArg(args, 1, 1000)
    applyObjectStoreConfigurationOptions(sparkConf)

    val spark = SparkSession
        .builder
        .appName("S3DataFrames")
        .config(sparkConf)
        .getOrCreate()

    import spark.implicits._

    val numRows = 1000

    try {
      val sc = spark.sparkContext
      val hConf = sc.hadoopConfiguration
      // simple benchmark code from DataSetBenchmark
      val sourceData = spark.range(0, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))

      val generatedBase = new Path(dest, "generated")
      // formats to generate
      val formats = Seq("orc", "parquet", "json", "csv")

      // write a DF
      def write(format: String): Path = {
        duration(s"write $format") {
          save(sourceData, new Path(generatedBase, format), format)
        }
      }
      // load a DF and verify it has the expected number of rows
      def validateRows(source: Path, srcFormat: String): Unit = {
        val loadedCount = load(spark, source, srcFormat).count()
        require(rowCount == loadedCount,
          s"Expected $rowCount rows, but got $loadedCount from $source formatted as $srcFormat")
      }

      val orc = write("orc")
      val parquet = write("parquet")
      write("json")
      write("csv")

      // convert a DF from one form to another
      def convert(convertBase: Path, source: Path, srcFormat: String, destFormat: String): Path = {
        val convertedDest = new Path(convertBase, s"$srcFormat-$destFormat")
        duration(s"save $source to $convertedDest as $destFormat") {
          val loadDF = load(spark, source, srcFormat)
          save(loadDF, convertedDest, destFormat)
        }
        validateRows(convertedDest, destFormat)
        convertedDest
      }

      val fromOrc = formats.map(convert(new Path(dest, "convertFromOrc"), orc, "orc", _))
      val fromParquet = formats.map(
        convert(new Path(dest, "convertFromParquet"), parquet, "parquet", _))

      // log any published filesystem state
      logInfo(s"FS: ${FileSystem.get(dest.toUri, hConf)}")
    } finally {
      spark.stop()
    }
    0
  }

}

private[cloud] object CloudDataFrames {

  def main(args: Array[String]) {
    new CloudDataFrames().main(args)
  }

}
