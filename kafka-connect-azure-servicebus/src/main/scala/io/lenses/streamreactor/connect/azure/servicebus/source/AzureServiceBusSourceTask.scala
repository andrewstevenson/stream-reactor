/*
 *
 *  * Copyright 2017 Datamountaineer.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package io.lenses.streamreactor.connect.azure.servicebus.source

import com.azure.messaging.servicebus.ServiceBusReceivedMessage
import com.datamountaineer.streamreactor.common.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.servicebus
import io.lenses.streamreactor.connect.azure.servicebus.config.{AzureServiceBusConfig, AzureServiceBusSettings}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer
import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

//noinspection VarCouldBeVal
class AzureServiceBusSourceTask extends SourceTask with StrictLogging {

  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation)

  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val recordsToCommit =
    new ConcurrentHashMap[SourceRecord, MessageAndTimestamp]()
  private var lastEvictedTimestamp: FiniteDuration =
    FiniteDuration(System.currentTimeMillis(), MILLISECONDS)
  private var evictInterval: Int = 0
  //noinspection VarCouldBeVal
  private var evictThreshold: Int = 0
  var reader: AzureServiceBusReader = _
  var nextRead: Long = 0L
  var interval: Long = AzureServiceBusConfig.AZURE_POLL_INTERVAL_DEFAULT

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      scala.io.Source
        .fromInputStream(getClass.getResourceAsStream("/servicebus-ascii.txt"))
        .mkString + s" ${version()}")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()
    AzureServiceBusConfig.config.parse(conf)
    val configBase = AzureServiceBusConfig(conf)
    val settings = AzureServiceBusSettings(configBase)

    interval = settings.pollInterval

    reader = AzureServiceBusReader(
      props.getOrDefault("name", ""),
      settings,
      servicebus.getConverters(settings.converters, conf.asScala.toMap)
    )
    enableProgress =
      configBase.getBoolean(AzureServiceBusConfig.PROGRESS_COUNTER_ENABLED)
  }

  override def poll(): util.List[SourceRecord] = {
    val now = System.currentTimeMillis()

    if (now > nextRead) {
      val records = reader.read().map { r =>
        recordsToCommit.put(
          r.record,
          MessageAndTimestamp(r.message,
            r.source,
            FiniteDuration(now,
              MILLISECONDS)))
        r.record
      }

      if (enableProgress) {
        progressCounter.update(records)
      }
      // increment the time
      nextRead = now + interval
      records.asJava

    } else {
      logger.info(s"Waiting for frequency to pass before polling, [${nextRead - now}] ms remaining")
      List.empty.asJava
    }
  }

  override def stop(): Unit = reader.close()

  override def version(): String = manifest.version()

  // once connect has committed complete the record
  override def commitRecord(record: SourceRecord): Unit = {
    Option(recordsToCommit.remove(record)).foreach {
      case MessageAndTimestamp(msgToken, source, _) =>
        reader.complete(source, msgToken)
    }
    evictUncommittedMessages()
  }

  def getRecordsToCommit: ConcurrentHashMap[SourceRecord, MessageAndTimestamp] =
    recordsToCommit

  private def evictUncommittedMessages(): Unit = {
    val current = FiniteDuration(System.currentTimeMillis(), MILLISECONDS)
    if ((current - lastEvictedTimestamp).toMinutes > evictInterval) {
      recordsToCommit.forEach(
        new BiConsumer[SourceRecord, MessageAndTimestamp] {
          override def accept(t: SourceRecord, u: MessageAndTimestamp): Unit =
            evictIfApplicable(t, u, current)
        })
    }
    lastEvictedTimestamp = current
  }

  private def evictIfApplicable(record: SourceRecord,
                                msg: MessageAndTimestamp,
                                now: FiniteDuration): Unit = {
    if ((now - msg.timestamp).toMinutes > evictThreshold) {
      recordsToCommit.remove(record)
    }
  }

  case class MessageAndTimestamp(message: ServiceBusReceivedMessage,
                                 source: String,
                                 timestamp: FiniteDuration)
}
