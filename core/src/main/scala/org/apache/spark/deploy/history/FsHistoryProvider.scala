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

package org.apache.spark.deploy.history

import java.io.{BufferedInputStream, FileNotFoundException, InputStream, IOException, OutputStream}
import java.util.UUID
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.mutable

import com.google.common.io.ByteStreams
import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider checks for new finished applications in the background periodically and
 * renders the history application UI by parsing the associated event logs.
 */
private[history] class FsHistoryProvider(conf: SparkConf, clock: Clock)
  extends ApplicationHistoryProvider with Logging {

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  import FsHistoryProvider._

  private val NOT_STARTED = "<Not Started>"

  // Interval between safemode checks.
  private val SAFEMODE_CHECK_INTERVAL_S = conf.getTimeAsSeconds(
    "spark.history.fs.safemodeCheck.interval", "5s")

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.update.interval", "10s")

  // Interval between each cleaner checks for event logs to delete
  private val CLEAN_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.cleaner.interval", "1d")

  private val logDir = conf.getOption("spark.history.fs.logDirectory")
    .map { d => Utils.resolveURI(d).toString }
    .getOrElse(DEFAULT_LOG_DIR)

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fs = Utils.getHadoopFileSystem(logDir, hadoopConf)

  // Used by check event thread and clean log thread.
  // Scheduled thread pool size must be one, otherwise it will have concurrent issues about fs
  // and applications between check task and clean task.
  private val pool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
    .setNameFormat("spark-history-task-%d").setDaemon(true).build())

  // The modification time of the newest log detected during the last scan. This is used
  // to ignore logs that are older during subsequent scans, to avoid processing data that
  // is already known.
  private var lastScanTime = -1L

  // Mapping of application IDs to their metadata, in descending end time order. Apps are inserted
  // into the map in order, so the LinkedHashMap maintains the correct ordering.
  @volatile private var applications: mutable.LinkedHashMap[String, FsApplicationHistoryInfo]
    = new mutable.LinkedHashMap()

  // List of application logs to be deleted by event log cleaner.
  private var attemptsToClean = new mutable.ListBuffer[FsApplicationAttemptInfo]

  /**
   * Return a runnable that performs the given operation on the event logs.
   * This operation is expected to be executed periodically.
   */
  private def getRunner(operateFun: () => Unit): Runnable = {
    new Runnable() {
      override def run(): Unit = Utils.tryOrExit {
        operateFun()
      }
    }
  }

  /**
   * An Executor to fetch and parse log files.
   */
  private val replayExecutor: ExecutorService = {
    if (!conf.contains("spark.testing")) {
      ThreadUtils.newDaemonSingleThreadExecutor("log-replay-executor")
    } else {
      MoreExecutors.sameThreadExecutor()
    }
  }

  // Conf option used for testing the initialization code.
  val initThread = initialize()

  private[history] def initialize(): Thread = {
    if (!isFsInSafeMode()) {
      startPolling()
      null
    } else {
      startSafeModeCheckThread(None)
    }
  }

  private[history] def startSafeModeCheckThread(
      errorHandler: Option[Thread.UncaughtExceptionHandler]): Thread = {
    // Cannot probe anything while the FS is in safe mode, so spawn a new thread that will wait
    // for the FS to leave safe mode before enabling polling. This allows the main history server
    // UI to be shown (so that the user can see the HDFS status).
    val initThread = new Thread(new Runnable() {
      override def run(): Unit = {
        try {
          while (isFsInSafeMode()) {
            logInfo("HDFS is still in safe mode. Waiting...")
            val deadline = clock.getTimeMillis() +
              TimeUnit.SECONDS.toMillis(SAFEMODE_CHECK_INTERVAL_S)
            clock.waitTillTime(deadline)
          }
          startPolling()
        } catch {
          case _: InterruptedException =>
        }
      }
    })
    initThread.setDaemon(true)
    initThread.setName(s"${getClass().getSimpleName()}-init")
    initThread.setUncaughtExceptionHandler(errorHandler.getOrElse(
      new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError("Error initializing FsHistoryProvider.", e)
          System.exit(1)
        }
      }))
    initThread.start()
    initThread
  }

  private def startPolling(): Unit = {
    // Validate the log directory.
    val path = new Path(logDir)
    if (!fs.exists(path)) {
      var msg = s"Log directory specified does not exist: $logDir."
      if (logDir == DEFAULT_LOG_DIR) {
        msg += " Did you configure the correct one through spark.history.fs.logDirectory?"
      }
      throw new IllegalArgumentException(msg)
    }
    if (!fs.getFileStatus(path).isDirectory) {
      throw new IllegalArgumentException(
        "Logging directory specified is not a directory: %s".format(logDir))
    }

    // Disable the background thread during tests.
    if (!conf.contains("spark.testing")) {
      // A task that periodically checks for event log updates on disk.
      logDebug(s"Scheduling update thread every $UPDATE_INTERVAL_S seconds")
      pool.scheduleWithFixedDelay(getRunner(checkForLogs), 0, UPDATE_INTERVAL_S, TimeUnit.SECONDS)

      if (conf.getBoolean("spark.history.fs.cleaner.enabled", false)) {
        // A task that periodically cleans event logs on disk.
        pool.scheduleWithFixedDelay(getRunner(cleanLogs), 0, CLEAN_INTERVAL_S, TimeUnit.SECONDS)
      }
    } else {
      logDebug("Background update thread disabled for testing")
    }
  }

  override def getListing(): Iterable[FsApplicationHistoryInfo] = applications.values

  /**
   * Look up an application attempt
   * @param appId application ID
   * @param attemptId Attempt ID, if set
   * @return the matching attempt, if found
   */
  def lookup(appId: String, attemptId: Option[String]): Option[FsApplicationAttemptInfo] = {
    applications.get(appId).flatMap { appInfo =>
      appInfo.attempts.find(_.attemptId == attemptId)
    }
  }

  override def getAppUI(appId: String, attemptId: Option[String])
      : Option[(SparkUI, Long, Option[Any])] = {
    try {
      applications.get(appId).flatMap { appInfo =>
        appInfo.attempts.find(_.attemptId == attemptId).flatMap { attempt =>
          val replayBus = new ReplayListenerBus()
          val ui = {
            val conf = this.conf.clone()
            val appSecManager = new SecurityManager(conf)
            SparkUI.createHistoryUI(conf, replayBus, appSecManager, appInfo.name,
              HistoryServer.getAttemptURI(appId, attempt.attemptId), attempt.startTime)
            // Do not call ui.bind() to avoid creating a new server for each application
          }
          val appListener = new ApplicationEventListener()
          replayBus.addListener(appListener)
          val status = fs.getFileStatus(new Path(logDir, attempt.logPath))
          val appAttemptInfo = replay(status,
            replayBus)
          appAttemptInfo.map { info =>
            val uiAclsEnabled = conf.getBoolean("spark.history.ui.acls.enable", false)
            ui.getSecurityManager.setAcls(uiAclsEnabled)
            // make sure to set admin acls before view acls so they are properly picked up
            ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
            ui.getSecurityManager.setViewAcls(attempt.sparkUser,
              appListener.viewAcls.getOrElse(""))
            (ui, Math.max(attempt.fileSizeUpdateTime, status.getModificationTime),
                Some(status.getLen))
          }
        }
      }
    } catch {
      case e: FileNotFoundException => None
    }
  }

  override def getConfig(): Map[String, String] = {
    val safeMode = if (isFsInSafeMode()) {
      Map("HDFS State" -> "In safe mode, application logs not available.")
    } else {
      Map()
    }
    Map("Event log directory" -> logDir.toString) ++ safeMode
  }

  override def stop(): Unit = {
    if (initThread != null && initThread.isAlive()) {
      initThread.interrupt()
      initThread.join()
    }
  }

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   */
  private[history] def checkForLogs(): Unit = {
    try {
      val newLastScanTime = getNewLastScanTime()
      logDebug(s"Scanning $logDir with lastScanTime==$lastScanTime")
      val statusList = Option(fs.listStatus(new Path(logDir))).map(_.toSeq)
        .getOrElse(Seq[FileStatus]())

      // scan for logs which have changed filesystem size. These have their
      // size and updated flags set, but are not replayed.
      // TODO

      // scan for modified applications, replay and merge them
      val logInfos: Seq[FileStatus] = statusList
        .filter { entry =>
          try {
            !entry.isDirectory() && (entry.getModificationTime() >= lastScanTime)
          } catch {
            case e: AccessControlException =>
              // Do not use "logInfo" since these messages can get pretty noisy if printed on
              // every poll.
              logDebug(s"No permission to read $entry, ignoring.")
              false
          }
        }
        .flatMap { entry => Some(entry) }
        .sortWith { case (entry1, entry2) =>
          entry1.getModificationTime() >= entry2.getModificationTime()
      }

      if (log.isDebugEnabled && logInfos.nonEmpty) {
        logDebug(s"New/updated attempts found: ${logInfos.size} ${logInfos.map(_.getPath)}")
      }
      logInfos.grouped(20)
        .map { batch =>
          replayExecutor.submit(new Runnable {
            override def run(): Unit = mergeApplicationListing(batch)
          })
        }
        .foreach { task =>
          try {
            // Wait for all tasks to finish. This makes sure that checkForLogs
            // is not scheduled again while some tasks are already running in
            // the replayExecutor.
            task.get()
          } catch {
            case e: InterruptedException =>
              throw e
            case e: Exception =>
              logError("Exception while merging application listings", e)
          }
        }
      // now scan for updated file sizes
      updateAttemptFileSizes()

      lastScanTime = newLastScanTime
    } catch {
      case e: Exception => logError("Exception in checking for event log updates", e)
    }
  }

  private def getNewLastScanTime(): Long = {
    val fileName = "." + UUID.randomUUID().toString
    val path = new Path(logDir, fileName)
    val fos = fs.create(path)

    try {
      fos.close()
      fs.getFileStatus(path).getModificationTime
    } catch {
      case e: Exception =>
        logError("Exception encountered when attempting to update last scan time", e)
        lastScanTime
    } finally {
      if (!fs.delete(path, true)) {
        logWarning(s"Error deleting ${path}")
      }
    }
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {

    /**
     * This method compresses the files passed in, and writes the compressed data out into the
     * [[OutputStream]] passed in. Each file is written as a new [[ZipEntry]] with its name being
     * the name of the file being compressed.
     */
    def zipFileToStream(file: Path, entryName: String, outputStream: ZipOutputStream): Unit = {
      val fs = FileSystem.get(hadoopConf)
      val inputStream = fs.open(file, 1 * 1024 * 1024) // 1MB Buffer
      try {
        outputStream.putNextEntry(new ZipEntry(entryName))
        ByteStreams.copy(inputStream, outputStream)
        outputStream.closeEntry()
      } finally {
        inputStream.close()
      }
    }

    applications.get(appId) match {
      case Some(appInfo) =>
        try {
          // If no attempt is specified, or there is no attemptId for attempts, return all attempts
          appInfo.attempts.filter { attempt =>
            attempt.attemptId.isEmpty || attemptId.isEmpty || attempt.attemptId.get == attemptId.get
          }.foreach { attempt =>
            val logPath = new Path(logDir, attempt.logPath)
            zipFileToStream(new Path(logDir, attempt.logPath), attempt.logPath, zipStream)
          }
        } finally {
          zipStream.close()
        }
      case None => throw new SparkException(s"Logs for $appId not found.")
    }
  }


  /**
   * Replay the log files in the list and merge the list of old applications with new ones
   */
  private def mergeApplicationListing(logs: Seq[FileStatus]): Unit = {
    val newAttempts = logs.flatMap { fileStatus =>
      try {
        val bus = new ReplayListenerBus()
        val res = replay(fileStatus, bus)
        res match {
          case Some(r) => logDebug(s"Application log ${r.logPath} loaded successfully: $r")
          case None => logWarning(s"Failed to load application log ${fileStatus.getPath}. " +
              "The application may have not started.")
        }
        res
      } catch {
        case e: Exception =>
          logError(
            s"Exception encountered when attempting to load application log ${fileStatus.getPath}",
            e)
          None
      }
    }

    if (newAttempts.nonEmpty) {
      applications = mergeAttempts(newAttempts, applications)
    }
  }

  /**
   * Build a map containing all apps that contain new attempts. The app information in this map
   * contains both the new app attempt, and those that were already loaded in the existing apps
   * map. If an attempt has been updated, it replaces the old attempt in the list.
   * The ordering is maintained
   * @param newAttempts new attempt list
   * @param current the current attempt list
   *       * @return the updated list
   */
  private def mergeAttempts(newAttempts: Iterable[FsApplicationAttemptInfo],
      current: mutable.LinkedHashMap[String, FsApplicationHistoryInfo])
      : mutable.LinkedHashMap[String, FsApplicationHistoryInfo] = {
    val newAppMap = new mutable.HashMap[String, FsApplicationHistoryInfo]()
    newAttempts.foreach { attempt =>
      val appInfo = newAppMap.get(attempt.appId)
        .orElse(current.get(attempt.appId))
        .map { app =>
          val attempts =
            app.attempts.filter(_.attemptId != attempt.attemptId).toList ++ List(attempt)
          var sortedAttempts = attempts.sortWith(compareAttemptInfo)
          new FsApplicationHistoryInfo(attempt.appId, attempt.name,
            sortedAttempts, sortedAttempts.head.lastUpdated)
        }
        .getOrElse(new FsApplicationHistoryInfo(attempt.appId, attempt.name, List(attempt),
          attempt.lastUpdated))
      newAppMap(attempt.appId) = appInfo
    }

    // Merge the new app list with the existing one, maintaining the expected ordering (descending
    // end time). Maintaining the order is important to avoid having to sort the list every time
    // there is a request for the log list.
    val newApps = newAppMap.values.toSeq.sortWith(compareAppInfo)
    val mergedApps = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()
    def addIfAbsent(info: FsApplicationHistoryInfo): Unit = {
      if (!mergedApps.contains(info.id)) {
        mergedApps += (info.id -> info)
      }
    }

    val newIterator = newApps.iterator.buffered
    val oldIterator = current.values.iterator.buffered
    while (newIterator.hasNext && oldIterator.hasNext) {
      if (newAppMap.contains(oldIterator.head.id)) {
        oldIterator.next()
      } else if (compareAppInfo(newIterator.head, oldIterator.head)) {
        addIfAbsent(newIterator.next())
      } else {
        addIfAbsent(oldIterator.next())
      }
    }
    newIterator.foreach(addIfAbsent)
    oldIterator.foreach(addIfAbsent)
    mergedApps
  }


  /**
   * Scan through all the application attempts, if any have changed those attempts
   * will be updated with the new file sizes. No attempt to replay the application
   * is made; this is a low cost operation.
   */
  private[history] def updateAttemptFileSizes(): Unit = {
    val now = System.currentTimeMillis();
    val newAttempts: Iterable[FsApplicationAttemptInfo] = applications
        .filter( e => !e._2.completed)
        .flatMap { e =>
          // build list of (false, attempt) or (true, attempt') values
          e._2.attempts.flatMap { attempt: FsApplicationAttemptInfo =>
            val path = new Path(logDir, attempt.logPath)
            try {
              val status = fs.getFileStatus(path)
              val size = getLogSize(status).getOrElse(-1L)
              val aS = attempt.fileSize
              if (size > aS) {
                logDebug(s"Attempt ${attempt.name}/${attempt.appId} size => $size")
                Some(new FsApplicationAttemptInfo(attempt.logPath, attempt.name, attempt.appId,
                  attempt.attemptId, attempt.startTime, attempt.endTime, attempt.lastUpdated,
                  attempt.sparkUser, attempt.completed, size, now))
              } else {
                None
              }
            } catch {
              case ex: FileNotFoundException =>
                // the file no longer exists
                // catching an FNFE is faster than doing exists() + getFileStatus(),
                // as exists() is usually getFileStatus() plus the catch.
                logInfo(s"missing file: $path")
                None
            }
          }
        }

    if (newAttempts.nonEmpty) {
      logDebug(s"Updating ${newAttempts.size} attempts from size changes")
      applications = mergeAttempts(newAttempts, applications)
    }
  }

  /**
   * Delete event logs from the log directory according to the clean policy defined by the user.
   */
  private[history] def cleanLogs(): Unit = {
    try {
      val maxAge = conf.getTimeAsSeconds("spark.history.fs.cleaner.maxAge", "7d") * 1000

      val now = clock.getTimeMillis()
      val appsToRetain = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()

      def shouldClean(attempt: FsApplicationAttemptInfo): Boolean = {
        now - attempt.lastUpdated > maxAge && attempt.completed
      }

      // Scan all logs from the log directory.
      // Only completed applications older than the specified max age will be deleted.
      applications.values.foreach { app =>
        val (toClean, toRetain) = app.attempts.partition(shouldClean)
        attemptsToClean ++= toClean

        if (toClean.isEmpty) {
          appsToRetain += (app.id -> app)
        } else if (toRetain.nonEmpty) {
          appsToRetain += (app.id ->
            new FsApplicationHistoryInfo(app.id, app.name, toRetain.toList, app.lastUpdated))
        }
      }

      applications = appsToRetain

      val leftToClean = new mutable.ListBuffer[FsApplicationAttemptInfo]
      attemptsToClean.foreach { attempt =>
        try {
          val path = new Path(logDir, attempt.logPath)
          if (fs.exists(path)) {
            if (!fs.delete(path, true)) {
              logWarning(s"Error deleting ${path}")
            }
          }
        } catch {
          case e: AccessControlException =>
            logInfo(s"No permission to delete ${attempt.logPath}, ignoring.")
          case t: IOException =>
            logError(s"IOException in cleaning ${attempt.logPath}", t)
            leftToClean += attempt
        }
      }

      attemptsToClean = leftToClean
    } catch {
      case t: Exception => logError("Exception in cleaning logs", t)
    }
  }

  /**
   * Comparison function that defines the sort order for the application listing.
   *
   * @return Whether `i1` should precede `i2`.
   */
  private def compareAppInfo(
      i1: FsApplicationHistoryInfo,
      i2: FsApplicationHistoryInfo): Boolean = {
    val a1 = i1.attempts.head
    val a2 = i2.attempts.head
    if (a1.endTime != a2.endTime) a1.endTime >= a2.endTime else a1.startTime >= a2.startTime
  }

  /**
   * Comparison function that defines the sort order for application attempts within the same
   * application. Order is: attempts are sorted by descending start time.
   * Most recent attempt state matches with current state of the app.
   *
   * Normally applications should have a single running attempt; but failure to call sc.stop()
   * may cause multiple running attempts to show up.
   *
   * @return Whether `a1` should precede `a2`.
   */
  private def compareAttemptInfo(
      a1: FsApplicationAttemptInfo,
      a2: FsApplicationAttemptInfo): Boolean = {
    a1.startTime >= a2.startTime
  }

  /**
   * Replays the events in the specified log file and returns information about the associated
   * application. Return `None` if the application ID cannot be located.
   */
  private def replay(
      eventLog: FileStatus,
      bus: ReplayListenerBus): Option[FsApplicationAttemptInfo] = {
    val logPath = eventLog.getPath()
    logInfo(s"Replaying log path: $logPath")
    val logInput = EventLoggingListener.openEventLog(logPath, fs)
    try {
      val appListener = new ApplicationEventListener
      val appCompleted = isApplicationCompleted(eventLog)
      bus.addListener(appListener)
      bus.replay(logInput, logPath.toString, !appCompleted)

      // Without an app ID, new logs will render incorrectly in the listing page, so do not list or
      // try to show their UI.
      if (appListener.appId.isDefined) {
        Some(new FsApplicationAttemptInfo(
          logPath.getName(),
          appListener.appName.getOrElse(NOT_STARTED),
          appListener.appId.getOrElse(logPath.getName()),
          appListener.appAttemptId,
          appListener.startTime.getOrElse(-1L),
          appListener.endTime.getOrElse(-1L),
          eventLog.getModificationTime(),
          appListener.sparkUser.getOrElse(NOT_STARTED),
          appCompleted,
          getLogSize(eventLog).getOrElse(0),
          eventLog.getModificationTime()))
      } else {
        None
      }
    } finally {
      logInput.close()
    }
  }

  /**
   * Get the size of the log, or `None` if there isn't one in the child
   * directory of a legacy log entry
   * @param fsEntry file status of a path
   * @return the log size
   */
  private def getLogSize(fsEntry: FileStatus): Option[Long] = {
    Some(fsEntry.getLen())
  }

  /**
   * Return true when the application has completed.
   */
  private def isApplicationCompleted(entry: FileStatus): Boolean = {
    !entry.getPath().getName().endsWith(EventLoggingListener.IN_PROGRESS)
  }

  /**
   * Checks whether HDFS is in safe mode.
   *
   * Note that DistributedFileSystem is a `@LimitedPrivate` class, which for all practical reasons
   * makes it more public than not.
   */
  private[history] def isFsInSafeMode(): Boolean = fs match {
    case dfs: DistributedFileSystem =>
      isFsInSafeMode(dfs)
    case _ =>
      false
  }

  // For testing.
  private[history] def isFsInSafeMode(dfs: DistributedFileSystem): Boolean = {
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET)
  }


/*
  def isCompleted(appId: String, attemptId: Option[String]): Boolean = {

    val name = appId + attemptId.map { id => s"_$id" }.getOrElse("")
    if (isAppCompleted.keySet.contains(name)) {
      true
    } else if (isAppCompleted.contains(name + EventLoggingListener.IN_PROGRESS)) {
      false
    } else {
      throw new NoSuchElementException(s"no app with key $appId/$attemptId.")
    }
  }
*/

  /**
   * String description for diagnostics
   * @return a summary of the component staet
   */
  override def toString: String = {
    val header = s"""
      | FsHistoryProvider: logdir=$logDir,
      | last scan time=$lastScanTime
      | Cached application count =${applications.size}}
      |
    """.stripMargin
    val sb = new StringBuilder(header)
    applications.foreach(entry => sb.append(entry._2).append("\n"))
    sb.toString
  }

  /**
   * Probe for an update to an (incompleted) application.
   * Here the check is "is the currently cached history info more up to date than the current one"
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @param updateTimeMillis time in milliseconds to use as the threshold for an update.
   * @param data a long containing the file size at the time of the last check
   * @return true if the application was updated since `updateTimeMillis`
   */
  override def isUpdated(appId: String, attemptId: Option[String], updateTimeMillis: Long,
      data: Option[Any]): Boolean = {
    val oldSize = data.getOrElse(-1L).asInstanceOf[Long]
    lookup(appId, attemptId) match {
      case None =>
        logDebug(s"Application Attempt $appId/$attemptId not found")
        false
      case Some(attempt) =>
        attempt.lastUpdated > updateTimeMillis ||
            (oldSize >= 0 && attempt.fileSize > oldSize)
      //      attempt.fileSizeUpdateTime > updateTimeMillis
    }
  }
}

private[history] object FsHistoryProvider {
  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"
}

private class FsApplicationAttemptInfo(
    val logPath: String,
    val name: String,
    val appId: String,
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean = true,
    val fileSize: Long = -1,
    val fileSizeUpdateTime: Long = -1)
  extends ApplicationAttemptInfo(attemptId, startTime, endTime,
    lastUpdated, sparkUser, completed) {
  override def toString: String = {
    s"FsApplicationAttemptInfo($logPath, $name, $appId," +
      s" ${super.toString}, $fileSize, $fileSizeUpdateTime"
  }
}

private class FsApplicationHistoryInfo(
    id: String,
    override val name: String,
    override val attempts: List[FsApplicationAttemptInfo],
    val lastUpdated: Long)
  extends ApplicationHistoryInfo(id, name, attempts) {
  override def toString: String = {
    s"FsApplicationHistoryInfo(lastUpdated = $lastUpdated, ${super.toString}"
  }
}
