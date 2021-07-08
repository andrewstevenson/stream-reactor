/*
 * Copyright 2020 lensesio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.azure.storage.sources

import com.datamountaineer.streamreactor.common.utils.{JarManifest, ProgressCounter}
import com.microsoft.azure.storage.queue.CloudQueueMessage
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage._
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer
import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

//noinspection VarCouldBeVal
class AzureQueueStorageSourceTask extends SourceTask with StrictLogging {

  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation)
  var reader: AzureQueueStorageReader = _
  private var enableProgress: Boolean = false
  private val progressCounter = new ProgressCounter

  private val recordsToCommit =
    new ConcurrentHashMap[SourceRecord, MessageAndTimestamp]()
  private var lastEvictedTimestamp: FiniteDuration =
    FiniteDuration(System.currentTimeMillis(), MILLISECONDS)
  //noinspection VarCouldBeVal
  private var evictInterval: Int = 0
  private var evictThreshold: Int = 0

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      scala.io.Source
        .fromInputStream(getClass.getResourceAsStream("/storage-ascii.txt"))
        .mkString + s" ${version()}")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()

    AzureStorageConfig.config.parse(conf)
    val configBase = new AzureStorageConfig(conf)
    val settings = AzureStorageSettings(configBase)

    reader = AzureQueueStorageReader(
      props.getOrDefault("name", ""),
      settings,
      getQueueClient(settings.account, settings.accountKey, settings.endpoint),
      getConverters(settings.converters, conf.asScala.toMap),
      version(),
      manifest.gitCommit(),
      manifest.gitRepo()
    )

    enableProgress =
      configBase.getBoolean(AzureStorageConfig.PROGRESS_COUNTER_ENABLED)


  }

  override def poll(): util.List[SourceRecord] = {

    val records = reader.read().map { r =>
      if (r.ack) {
        recordsToCommit.put(
          r.record,
          MessageAndTimestamp(r.message,
                              r.source,
                              FiniteDuration(System.currentTimeMillis(),
                                             MILLISECONDS)))
      }
      r.record
    }

    if (enableProgress) {
      progressCounter.update(records)
    }

    records.asJava
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  // once connect has committed complete the record
  override def commitRecord(record: SourceRecord): Unit = {
    Option(recordsToCommit.remove(record)).foreach {
      case MessageAndTimestamp(msg, source, _) =>
        reader.commit(source, msg)
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

  case class MessageAndTimestamp(message: CloudQueueMessage,
                                 source: String,
                                 timestamp: FiniteDuration)
}
