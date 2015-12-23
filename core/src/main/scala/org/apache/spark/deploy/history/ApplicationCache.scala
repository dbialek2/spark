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

import scala.collection.JavaConverters._

import com.codahale.metrics.{Timer, Counter, MetricRegistry}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}

import org.apache.spark.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Clock

/**
 * Cache for applications.
 *
 * Completed applications are cached for as long as there is capacity for them.
 * Incompleted applications have their update time checked on every
 * retrieval; if the cached entry is out of date, it is refreshed.
 *
 * @param operations implementation of record access operations
 * @param refreshInterval interval between refreshes in nanoseconds.
 * @param retainedApplications number of retained applications
 * @param time time source
 */
private[history] class ApplicationCache(operations: ApplicationCacheOperations,
    val refreshInterval: Long,
    val retainedApplications: Int,
    time: Clock) extends Logging with Source {

  /**
   * Services the load request from the cache.
   */
  private val appLoader = new CacheLoader[CacheKey, CacheEntry] {

    /** the cache key doesn't match an cached entry ... attempt to load it  */
    override def load(key: CacheKey): CacheEntry = {
      loadApplicationEntry(key.appId, key.attemptId)
    }
  }

  private val removalListener = new RemovalListener[CacheKey, CacheEntry] {

    /**
     * Removal event notifies the provider to detach the UI
     * @param rm removal notification
     */
    override def onRemoval(rm: RemovalNotification[CacheKey, CacheEntry]): Unit = {
      removalCount.inc()
      operations.detachSparkUI(rm.getKey.appId, rm.getKey.attemptId, rm.getValue().ui)
    }
  }

  /**
   * The cache of applications.
   */
  private val appCache: LoadingCache[CacheKey, CacheEntry] = CacheBuilder.newBuilder()
      .maximumSize(retainedApplications)
      .removalListener(removalListener)
      .build(appLoader)

  val lookupCount = new Counter()
  val lookupFailureCount = new Counter()
  val removalCount = new Counter()
  val loadCount = new Counter()
  val loadTimer = new Timer()
  val updateProbeCount = new Counter()
  val updateProbeTimer = new Timer()
  val updateTriggeredCount = new Counter()

  private val counters = Seq(
    ("lookup.count", lookupCount),
    ("lookup.failure.count", lookupFailureCount),
    ("removal.count", removalCount),
    ("load.count", loadCount),
    ("update.probe.count", updateProbeCount),
    ("update.triggered.count", updateTriggeredCount))

  private val metrics = counters ++ Seq(
    ("load.timer", loadTimer),
    ("update.probe.timer", updateProbeTimer))

  override val sourceName = "ApplicationCache"

  override def metricRegistry = new MetricRegistry;

  // init operations
  init()

  private def init(): Unit = {
    metrics.foreach( e =>
      metricRegistry.register(MetricRegistry.name("history.cache", e._1), e._2))
  }

  private def time[T](t: Timer)(f: => T): T = {
    val timeCtx = t.time()
    try {
      f
    } finally {
      timeCtx.close()
    }
  }

  /**
   * Load a cache entry, including registering the UI
   *
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the cache entry
   */
  def loadApplicationEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    logDebug(s"Loading application Entry $appId/$attemptId")
    loadCount.inc()
    time(loadTimer) {
      operations.getAppUI(appId, attemptId) match {
        case Some(ui) =>
          val completed = ui.getApplicationInfoList.exists(_.attempts.last.completed)
          // attach the spark UI
          operations.attachSparkUI(appId, attemptId, ui, completed)
          // build the cache entry
          new CacheEntry(ui, completed, time.getTimeMillis())
        case None =>
          lookupFailureCount.inc()
          throw new NoSuchElementException(s"no application with application Id '$appId'" +
              attemptId.map { id => s" attemptId '$id'" }.getOrElse(" and no attempt Id"))
      }
    }
  }

  /**
   * Split up an `applicationId/attemptId` or `applicationId` key into the separate pieces.
   *
   * @param appAndAttempt combined key
   * @return a tuple of the application ID and, if present, the attemptID
   */
  def splitAppAndAttemptKey(appAndAttempt: String): (String, Option[String]) = {
    val parts = appAndAttempt.split("/")
    require(parts.length == 1 || parts.length == 2, s"Invalid app key $appAndAttempt")
    val appId = parts(0)
    val attemptId = if (parts.length > 1) Some(parts(1)) else None
    (appId, attemptId)
  }

  /**
   * Merge an appId and optional attempt ID into a key of the form `applicationId/attemptId`
   * if there is an attempt ID; `applicationId` if not
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return a unified string
   */
  def mergeAppAndAttemptToKey(appId: String, attemptId: Option[String]) : String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }

  /**
   * Get the entry. Cache fetch/refresh will have taken place by
   * the time this method returns
   * @param appAndAttempt application to look up
   * @return the entry
   */
  def get(appAndAttempt: String): SparkUI = {
    val parts = splitAppAndAttemptKey(appAndAttempt)
    get(parts._1, parts._2)
  }

  /**
   * Get the associated spark UI. Cache fetch/refresh will have taken place by
   * the time this method returns
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the entry
   */
  def get(appId: String, attemptId: Option[String]): SparkUI = {
    lookupAndUpdate(appId, attemptId).ui
  }

  /**
   * Look up the entry; update it if needed. Returns the underlying cache entry -which
   * can have its timestamp changed
   * the time this method returns
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the entry
   */
  private def lookupAndUpdate(appId: String, attemptId: Option[String]): CacheEntry = {
    lookupCount.inc()
    val cacheKey = CacheKey(appId, attemptId)
    var entry = appCache.getIfPresent(cacheKey)
    if (entry == null) {
      // no entry, so fetch without any post-fetch probes for out-of-dateness
      entry = appCache.get(cacheKey)
    } else if (!entry.completed) {
      val now = time.getTimeMillis()
      if (now - entry.timestamp > refreshInterval) {
        log.debug(s"Probing for updated application $cacheKey")
        updateProbeCount.inc()
        val updated = time(updateProbeTimer) {
          operations.isUpdated(appId, attemptId, entry.timestamp)
        }
        if (updated) {
          logDebug(s"refreshing $cacheKey")
          updateTriggeredCount.inc()
          appCache.refresh(cacheKey)
          // and re-attempt the lookup
          entry = appCache.get(cacheKey)
        } else {
          // update the timestamp to the time of this probe
          entry.timestamp = now
        }
      }
    }
    entry
  }

  /**
   * This method is visible for testing. It looks up the cached entry *and returns a clone of it*.
   * This ensures that the cached entries never leak
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return a new entry with shared SparkUI, but copies of the other fields.
   */
  def lookupCacheEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    val entry = lookupAndUpdate(appId, attemptId)
    new CacheEntry(entry.ui, entry.completed, entry.timestamp)
  }

  /**
   * String operator dumps the cache entries and metrics
   * Warning: no concurrency guarantees.
   * @return a string value, primarily for testing and diagnostics
   */
  override def toString: String = {
    val sb = new StringBuilder(
      s"ApplicationCache($refreshInterval, $retainedApplications) size ${appCache.size()}\n")
    sb.append("----\n")
    appCache.asMap().asScala.foreach {
      case(key, entry) => sb.append(s"    $key -> $entry\n")
    }
    sb.append("----\n")
    counters.foreach{ e =>
        sb.append(e._1).append(" = ").append(e._2.getCount).append('\n')
    }
    sb.toString()
  }
}

/**
 * An entry in the cache.
 *
 * @param ui Spark UI
 * @param completed: flag to indicated that the application has completed (and so
 *                 does not need refreshing)
 * @param timestamp timestamp in milliseconds. This may be updated during probes
 */
private[history] final class CacheEntry(val ui: SparkUI, val completed: Boolean, var timestamp: Long) {

  /** string value is for test assertions */
  override def toString: String = {
    s"UI $ui, completed=$completed, timestamp=$timestamp"
  }
}

/**
 * Cache key: compares on App Id and then, if non-empty, attemptId.
 * Hash code is similar
 * @param appId application ID
 * @param attemptId attempt ID
 */
private[history] final case class CacheKey(appId: String, attemptId: Option[String]) {

  override def hashCode(): Int = {
    appId.hashCode() + attemptId.map(_.hashCode).getOrElse(0)
  }

  override def equals(obj: scala.Any): Boolean = {
    val that = obj.asInstanceOf[CacheKey]
    that.appId == appId && that.attemptId == attemptId
  }

  override def toString: String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }
}

/**
 * API for cache events. That is: loading an APP UI; probing for it changing, and for
 * attaching/detaching the UI to and from the Web UI.
 */
private[history] trait ApplicationCacheOperations {

  /**
   * Get the application UI
   * @param appId application ID
   * @param attemptId attempt ID
   * @return The Spark UI
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI]

  /**
   *  Attach a reconstructed UI.
   * @param appId application ID
   * @param attemptId attempt ID
   * @param ui UI
   * @param completed flag to indicate that the UI has completed
   */
  def attachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI, completed: Boolean): Unit

  /**
   *  Detach a reconstructed UI
   *
   * @param ui Spark UI
   */
  def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit

  /**
   * Probe for an update to an (incompleted) application
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @param updateTimeMillis time in milliseconds to use as the threshold for an update.
   * @return true if the application was updated since `updateTimeMillis`
   */
  def isUpdated(appId: String, attemptId: Option[String], updateTimeMillis: Long): Boolean
}
