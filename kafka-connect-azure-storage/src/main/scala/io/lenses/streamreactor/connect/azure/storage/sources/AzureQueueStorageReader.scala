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

import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.microsoft.azure.storage.queue.{CloudQueueClient, CloudQueueMessage}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}
import io.lenses.streamreactor.connect.azure.storage.getQueueReferences
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object AzureQueueStorageReader {
  def apply(name: String,
            settings: AzureStorageSettings,
            queueClient: CloudQueueClient,
            convertersMap: Map[String, Converter],
            version: String = "",
            gitCommit: String = "",
            gitRepo: String = ""): AzureQueueStorageReader =
    new AzureQueueStorageReader(name, settings, queueClient, convertersMap)
}

case class MessageAndSourceRecord(ack: Boolean,
                                  message: CloudQueueMessage,
                                  source: String,
                                  record: SourceRecord)

class AzureQueueStorageReader(name: String,
                              settings: AzureStorageSettings,
                              queueClient: CloudQueueClient,
                              convertersMap: Map[String, Converter])
    extends StrictLogging {

  private val cloudQueueMap = {
    val queueConfigs = settings.projections.targets.map {
      case (queue, topic) => queue -> (settings.projections.autoCreate(queue), false)
    }
    getQueueReferences(queueClient, queueConfigs).values.toList
  }

  // remove the message from the queue once connect commits
  def commit(source: String, message: CloudQueueMessage): Unit = {
    cloudQueueMap
      .filter(c => c.getName.equals(source))
      .foreach(c => {
        logger.debug(
          s"Removing message [${message.getId}] from queue [${c.getName}]")
        c.deleteMessage(message)
      })
  }

  def read(): List[MessageAndSourceRecord] = {

    cloudQueueMap.flatMap { client =>
      val queueName = client.getName
      val converter = convertersMap.getOrElse(
        queueName,
        throw new RuntimeException(
          s"Converter for [$queueName] defined in [${AzureStorageConfig.KCQL}]"))

      val batchSize =
        if (settings.projections.batchSize(queueName) > AzureStorageConfig.QUEUE_SOURCE_MAX_BATCH_SIZE)
          AzureStorageConfig.QUEUE_SOURCE_MAX_BATCH_SIZE
        else settings.projections.batchSize(queueName)

      client
        .retrieveMessages(batchSize, settings.lock(queueName), null, null)
        .asScala
        .map(m => {

          // to decode possible base64
          val payload = Try(m.getMessageContentAsByte) match {
            case Success(p) => p
            case Failure(_) =>
              m.getMessageContentAsString.getBytes
          }

          val headers = new ConnectHeaders()

          if (settings.setHeaders) {

            Option(m.getMessageId)
              .filterNot(_.isEmpty)
              .foreach(m =>
                headers.addString(AzureStorageConfig.HEADER_MESSAGE_ID, m))
            Option(m.getDequeueCount).foreach(m =>
              headers.addInt(AzureStorageConfig.HEADER_DEQUEUE_COUNT, m))
            headers.addBoolean(AzureStorageConfig.HEADER_REMOVED,
                               settings.projections.acks(queueName))
          }

          val convertedRecord = converter.convert(
            settings.projections.targets(queueName),
            queueName.replaceAll("-", "_"),
            Option(m.getMessageId).getOrElse(""),
            payload,
            settings.projections.keys(queueName),
            settings.projections.keyDelimiters(queueName)
          )

          val sourceRecord = convertedRecord.newRecord(
            convertedRecord.topic(),
            convertedRecord.kafkaPartition(),
            convertedRecord.keySchema(),
            convertedRecord.key(),
            convertedRecord.valueSchema(),
            convertedRecord.value(),
            convertedRecord.timestamp(),
            headers
          )

          MessageAndSourceRecord(
            ack = settings.projections.acks(queueName),
            message = m,
            source = queueName,
            record = sourceRecord.newRecord(
              sourceRecord.topic(),
              sourceRecord.kafkaPartition(),
              sourceRecord.keySchema(),
              sourceRecord.key(),
              sourceRecord.valueSchema(),
              sourceRecord.value(),
              sourceRecord.timestamp(),
              headers
            )
          )
        })
    }
  }
}
