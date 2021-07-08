/*
 *
 *  * Copyright 2020 Lenses.io.
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

package io.lenses.streamreactor.connect.azure.storage.sinks.writers

import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.microsoft.azure.storage.queue.CloudQueueClient
import com.microsoft.azure.storage.table.CloudTableClient
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.TargetType.TargetType
import io.lenses.streamreactor.connect.azure.storage._
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageSettings
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.{Failure, Success, Try}

object AzureStorageWriter {
  def apply(settings: AzureStorageSettings) = new AzureStorageWriter(settings)
}

class AzureStorageWriter(settings: AzureStorageSettings)
    extends StrictLogging
    with ErrorHandler {

  var writers = Map.empty[String, Writer]
  private val COSMOS_TABLE_ENDPOINT_TYPE = "TableEndpoint"

  //initialize error tracker
  initialize(settings.projections.errorRetries, settings.projections.errorPolicy)

  // group by topic and batch to send to the writer
  def write(records: Seq[SinkRecord]): Unit = {

    // set up the writers if not set
    if (writers.isEmpty) writers = initializeWriters

    val grouped =
      records.groupBy(r => new TopicPartition(r.topic(), r.kafkaPartition()))

    grouped.foreach({
      case (topicPartition, group) =>
        val writer = writers(topicPartition.topic())
        val targetName = settings.projections.targets(topicPartition.topic())
        val batchSize = settings.projections.batchSize(topicPartition.topic())
        writer.write(targetName, group, batchSize)
    })
  }

  //create the azure cloud clients
  private def initializeClient(
      settings: AzureStorageSettings,
      targetType: TargetType): Either[CloudTableClient, CloudQueueClient] = {
    val storageAccount = getStorageAccount(settings.account,
                                           settings.accountKey,
                                           settings.endpoint,
                                           COSMOS_TABLE_ENDPOINT_TYPE)

    targetType match {
      case TargetType.TABLE =>
        Try(storageAccount.createCloudTableClient()) match {
          case Success(client) => Left(client)
          case Failure(exception) =>
            throw new ConnectException("Failed to create cloud table client",
                                       exception)
        }
      case TargetType.QUEUE =>
        Try(storageAccount.createCloudQueueClient()) match {
          case Success(client) => Right(client)
          case Failure(exception) =>
            throw new ConnectException("Failed to create cloud table client",
                                       exception)
        }
    }
  }

  //get writers, check the map, if not found initialize so we can mock
  private def initializeWriters: Map[String, Writer] = {
    settings.targetType.map({
      case (topic, targetType) =>
        targetType match {
          case TargetType.QUEUE =>
            topic -> writers.getOrElse(
              topic,
              AzureQueueStorageWriter(settings, getQueueWriter))
          case TargetType.TABLE =>
            topic -> writers.getOrElse(
              topic,
              AzureTableStorageWriter(settings, getTableWriter))
        }
    })
  }

  private def getQueueWriter: CloudQueueClient = {
    initializeClient(settings, TargetType.QUEUE) match {
      case Right(value) => value
      case _            => throw new ConnectException("Not a queue writer")
    }
  }

  private def getTableWriter: CloudTableClient = {
    initializeClient(settings, TargetType.TABLE) match {
      case Left(value) => value
      case _           => throw new ConnectException("Not a table writer")
    }
  }
}
