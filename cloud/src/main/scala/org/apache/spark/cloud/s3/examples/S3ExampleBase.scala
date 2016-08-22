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
import org.apache.spark.cloud.s3.S3AConstants
import org.apache.spark.cloud.utils.{ObjectStoreOperations, TimeOperations}

/**
 * Base Class for examples working with S3.
 */
private[cloud] trait S3ExampleBase extends TimeOperations with ObjectStoreOperations {
  /**
   * Default source of a public multi-MB CSV file.
   */
  val S3A_CSV_PATH_DEFAULT = "s3a://landsat-pds/scene_list.gz"

  /**
   * Exit code for a usage error.
   */
  val EXIT_USAGE = -2

  /**
   * Exit code for a general purpose error
   */
  val EXIT_ERROR = -1

  /**
   *
   * Any exception raised is logged at error and then the exit code set to -1.
   * @param args argument array
   */
  def main(args: Array[String]) {
    execute(action, args)
  }


  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  def action(sparkConf: SparkConf, args: Array[String]): Int = {
    0
  }

  /**
   * Parameter overridden action operation; easy to use in tests.
   * @param sparkConf configuration to use
   * @param args list of arguments -they are converted to strings before use
   * @return an exit code
   */
  def action(sparkConf: SparkConf, args: Seq[Any]): Int = {
    action(sparkConf,  args.map(_.toString).toArray)
  }

  /**
   * Execute an operation, using its return value as the System exit code.
   * Exceptions are caught, logged and an exit code of -1 generated.
   *
   * @param operation operation to execute
   * @param args list of arguments from the command line
   */
  protected def execute(operation: (SparkConf, Array[String]) => Int, args: Array[String]): Unit = {
    var exitCode = 0
    try {
      val conf = new SparkConf()
      exitCode = operation(conf, args)
    } catch {
      case e: Exception =>
        logError(s"Failed to execute operation: $e", e)
        // in case this is caused by classpath problems, dump it out
        logDebug(s"Classpath =\n${System.getProperty("java.class.path")}")
        exitCode = EXIT_ERROR
    }
    logInfo(s"Exit code = $exitCode")
    if (exitCode != 0) {
      exit(exitCode)
    }
  }

  /**
   * Set a hadoop option in a spark configuration.
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */
  def hconf(sparkConf: SparkConf, k: String, v: String): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v)
  }

  /**
   * Set a long hadoop option in a spark configuration.
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */
  def hconf(sparkConf: SparkConf, k: String, v: Long): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v.toString)
  }

  /**
   * Exit the system.
   * This may be overriden for tests: code must not assume that it never returns.
   * @param exitCode exit code to exit with.
   */
  def exit(exitCode: Int): Unit = {
    System.exit(exitCode)
  }

  protected def intArg(args: Array[String], index: Int, defVal: Int): Int = {
    if (args.length > index) args(index).toInt else defVal
  }
  protected def arg(args: Array[String], index: Int, defVal: String): String = {
    if (args.length > index) args(index) else defVal
  }

  protected def arg(args: Array[String], index: Int): Option[String] = {
    if (args.length > index) Some(args(index)) else None
  }

  /**
   * Set the standard S3A Hadoop options to be used in test/examples
   * @param sparkConf spark configuration to patch
   */
  protected def applyS3AConfigOptions(sparkConf: SparkConf): Unit = {
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
