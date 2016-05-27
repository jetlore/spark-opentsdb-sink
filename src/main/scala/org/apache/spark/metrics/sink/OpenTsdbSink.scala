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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.github.sps.metrics.OpenTsdbReporter
import com.github.sps.metrics.opentsdb.OpenTsdb
import org.apache.spark.{Logging, SecurityManager, SparkContext, SparkEnv}
import org.apache.spark.metrics.MetricsSystem

/**
  * This file is taken from pull request to Spark
  * https://github.com/apache/spark/pull/10187/commits/df43bb46d055611167c1253a497ff36f23248733
  * Issue: https://issues.apache.org/jira/browse/SPARK-12194
  *
  * Properties template:
  * # org.apache.spark.metrics.sink.OpenTsdbSink
  * #   Name:       Default:            Description:
  * #   host        NONE                Hostname of OpenTsdb server
  * #   port        NONE                Port of OpenTsdb server
  * #   period      10                  Poll period
  * #   unit        seconds             Units of poll period
  * #   prefix      EMPTY STRING        Prefix to prepend to metric name
  * #   tagName1    appId               Name of one required tag for OpenTsdb
  * #   tagValue1   SPARK APP ID        Value of one required tag for OpenTsdb
  */
private[spark] class OpenTsdbSink(val property: Properties, val registry: MetricRegistry,
                                  securityMgr: SecurityManager) extends Sink with Logging {

  logInfo("OpenTsdbSink initialized")

  val OPENTSDB_DEFAULT_PERIOD = 10
  val OPENTSDB_DEFAULT_UNIT = "SECONDS"
  val OPENTSDB_DEFAULT_PREFIX = ""
  val OPENTSDB_DEFAULT_TAG_NAME_1 = "appId"
  lazy val OPENTSDB_DEFAULT_TAG_VALUE_1 = Option(SparkEnv.get).map(_.conf).map(_.getAppId).getOrElse("unknown")

  val OPENTSDB_KEY_HOST = "host"
  val OPENTSDB_KEY_PORT = "port"
  val OPENTSDB_KEY_PERIOD = "period"
  val OPENTSDB_KEY_UNIT = "unit"
  val OPENTSDB_KEY_PREFIX = "prefix"

  val OPENTSDB_KEY_TAG_NAME_1 = "tagName1"
  val OPENTSDB_KEY_TAG_VALUE_1 = "tagValue1"
  val OPENTSDB_KEY_TAG_NAME_2 = "tagName2"
  val OPENTSDB_KEY_TAG_VALUE_2 = "tagValue2"
  val OPENTSDB_KEY_TAG_NAME_3 = "tagName3"
  val OPENTSDB_KEY_TAG_VALUE_3 = "tagValue3"
  val OPENTSDB_KEY_TAG_NAME_4 = "tagName4"
  val OPENTSDB_KEY_TAG_VALUE_4 = "tagValue4"
  val OPENTSDB_KEY_TAG_NAME_5 = "tagName5"
  val OPENTSDB_KEY_TAG_VALUE_5 = "tagValue5"
  val OPENTSDB_KEY_TAG_NAME_6 = "tagName6"
  val OPENTSDB_KEY_TAG_VALUE_6 = "tagValue6"
  val OPENTSDB_KEY_TAG_NAME_7 = "tagName7"
  val OPENTSDB_KEY_TAG_VALUE_7 = "tagValue7"
  val OPENTSDB_KEY_TAG_NAME_8 = "tagName8"
  val OPENTSDB_KEY_TAG_VALUE_8 = "tagValue8"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (!propertyToOption(OPENTSDB_KEY_HOST).isDefined) {
    throw new Exception(s"OpenTSDB sink requires '$OPENTSDB_KEY_HOST' property.")
  }

  if (!propertyToOption(OPENTSDB_KEY_PORT).isDefined) {
    throw new Exception(s"OpenTSDB sink requires '$OPENTSDB_KEY_PORT' property.")
  }

  val host = propertyToOption(OPENTSDB_KEY_HOST).get
  val port = propertyToOption(OPENTSDB_KEY_PORT).get.toInt

  val pollPeriod = propertyToOption(OPENTSDB_KEY_PERIOD) match {
    case Some(s) => s.toInt
    case None => OPENTSDB_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = propertyToOption(OPENTSDB_KEY_UNIT) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(OPENTSDB_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val prefix = propertyToOption(OPENTSDB_KEY_PREFIX).getOrElse(OPENTSDB_DEFAULT_PREFIX)

  val tagName1 = propertyToOption(OPENTSDB_KEY_TAG_NAME_1) match {
    case Some(n) => n
    case None =>
      logWarning(
        s"""'$OPENTSDB_KEY_TAG_NAME_1' property not specified for OpenTSDB sink,
            | using '$OPENTSDB_DEFAULT_TAG_NAME_1'"""".stripMargin)
      OPENTSDB_DEFAULT_TAG_NAME_1
  }

  lazy val tagValue1 = propertyToOption(OPENTSDB_KEY_TAG_NAME_1) match {
    case Some(n) =>
      propertyToOption(OPENTSDB_KEY_TAG_VALUE_1) match {
        case Some(v) => v
        case None =>
          logWarning(
            s"""'$OPENTSDB_KEY_TAG_VALUE_1' property not specified for OpenTSDB sink,
                |using '$OPENTSDB_DEFAULT_TAG_VALUE_1'""".stripMargin)
          OPENTSDB_DEFAULT_TAG_VALUE_1
      }
    case None =>
      propertyToOption(OPENTSDB_KEY_TAG_VALUE_1) match {
        case Some(v) =>
          throw new Exception(
            s"""'$OPENTSDB_KEY_TAG_VALUE_1' property cannot be specified for OpenTSDB sink
                |without specifying '$OPENTSDB_KEY_TAG_NAME_1' property.""".stripMargin)
        case None =>
          logWarning(
            s"""'$OPENTSDB_KEY_TAG_VALUE_1' property not specified for OpenTSDB sink,
                |using '$OPENTSDB_DEFAULT_TAG_VALUE_1'""".stripMargin)
          OPENTSDB_DEFAULT_TAG_VALUE_1
      }
  }

  private val tags = new java.util.HashMap[String, String]()

  private def getTags(registry: MetricRegistry): java.util.Map[String, String] = {
    tags.put(tagName1, tagValue1)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_2, OPENTSDB_KEY_TAG_VALUE_2)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_3, OPENTSDB_KEY_TAG_VALUE_3)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_4, OPENTSDB_KEY_TAG_VALUE_4)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_5, OPENTSDB_KEY_TAG_VALUE_5)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_6, OPENTSDB_KEY_TAG_VALUE_6)

    if (appId != null) {
      tags.put("appId", appId)
      tags.put("executorId", executorId)
    } else {
      updateTagsForTag(OPENTSDB_KEY_TAG_NAME_7, OPENTSDB_KEY_TAG_VALUE_7)
      updateTagsForTag(OPENTSDB_KEY_TAG_NAME_8, OPENTSDB_KEY_TAG_VALUE_8)
    }

    tags
  }

  private def updateTagsForTag(tagName: String, tagValue: String): Unit = {
    propertyToOption(tagName) match {
      case Some(n) => propertyToOption(tagValue) match {
        case Some(v) => tags.put(n, v)
        case None =>
          throw new Exception(
            s"OpenTSDB sink requires '$tagValue' property when '$tagName' property is specified."
          )
      }
      case None =>
    }
  }

  // extract appId and executorId from first metric's name
  var appId: String = null
  var executorId: String = null

  if (!registry.getNames.isEmpty) {
    // search strings like app-20160526104713-0016.0.xxx or app-20160526104713-0016.driver.xxx
    val pattern = """(app-\d+-\d+)\.(\d+|driver)\..+""".r

    registry.getNames.first match {
      case pattern(app, executor) =>
        this.appId = app
        this.executorId = executor
      case _ =>
    }
  }

  val openTsdb = OpenTsdb.forService("http://" + host + ":" + port).create()

  lazy val reporter: OpenTsdbReporter = OpenTsdbReporter.forRegistry(registry)
    .prefixedWith(prefix)
    .removePrefix(Option(appId).map(_ + "." + executorId + ".").orNull)
    .withTags(getTags(registry))
    .build(openTsdb)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}