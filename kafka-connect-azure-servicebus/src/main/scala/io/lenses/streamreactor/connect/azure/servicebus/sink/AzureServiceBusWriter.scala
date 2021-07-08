/*
 * Copyright 2017 Datamountaineer.
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

package io.lenses.streamreactor.connect.azure.servicebus.sink

import com.azure.messaging.servicebus._
import com.azure.messaging.servicebus.administration.models.{CreateQueueOptions, CreateTopicOptions}
import com.azure.messaging.servicebus.administration.{ServiceBusAdministrationClient, ServiceBusAdministrationClientBuilder}
import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.common.rowkeys._
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.servicebus
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSettings
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.{Failure, Success, Try}

object AzureServiceBusWriter {
  def apply(settings: AzureServiceBusSettings): AzureServiceBusWriter =
    new AzureServiceBusWriter(settings)
}

class AzureServiceBusWriter(settings: AzureServiceBusSettings)
    extends ErrorHandler
    with StrictLogging
{

  val jsonConverter = new SimpleJsonConverter()
  //noinspection VarCouldBeVal
  var clients: Map[String, ServiceBusSenderAsyncClient] = Map.empty

  //initialize error tracker
  initialize(settings.projections.errorRetries, settings.projections.errorPolicy)

  def close(): Unit = clients.values.foreach(_.close())

  // group by topic and send
  def write(records: Seq[SinkRecord]): Unit = {

    // if the clients aren't set initialize them
    if (clients.isEmpty) buildServiceBusClients()

    records
      .groupBy(r => new TopicPartition(r.topic(), r.kafkaPartition()))
      .foreach {
        case (topicPartition, groupedRecords) =>
          val client = clients.getOrElse(
            topicPartition.topic(),
            throw new ConnectException(
              s"Client not set for topic [${topicPartition.topic()}]"))

          //batch the records
          val batched =
            groupedRecords.grouped(settings.projections.batchSize(topicPartition.topic()))

          batched.foreach { g =>
            val messageBatch = client.createMessageBatch().block()

            g.map(r => convertToServiceBusMessage(r))
              .foreach(sbm => messageBatch.tryAddMessage(sbm))

            handleTry(Try(client.sendMessages(messageBatch).block()))
          }
      }
  }

  //convert the sinkrecord to a azure message
  def convertToServiceBusMessage(record: SinkRecord): ServiceBusMessage = {
    val filtered = record.newFilteredRecordAsStruct(settings.projections)
    val json = jsonConverter.fromConnectData(filtered.schema(), filtered).toString.getBytes
    val message = new ServiceBusMessage(json)
    message.setContentType("application/json")
    message.setMessageId(new StringGenericRowKeyBuilder("-").build(record))

    //set the session otherwise use kcql partitioning
    settings.projections.sessions.get(record.topic()) match {
      case Some(session) =>
        // https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-partitioning#using-a-partition-key
        // if a session is requested set the partition key to be the same
        message.setPartitionKey(session)
        message.setSessionId(session)
      case _ =>
        // if we have partition keys set them else use the message id

        settings.projections.partitionBy.get(record.topic()) match {
          case Some(pks) =>
            val partitionKey = StringStructFieldsStringKeyBuilder(
              pks.toSeq,
              settings.projections.keyDelimiters.getOrElse(record.topic(), "-"))
              .build(record)
            message.setPartitionKey(partitionKey)

          case _ =>
            message.setPartitionKey(message.getMessageId)
        }
    }

    //set ttl
    settings.projections.ttl.get(record.topic()) match {
      case Some(ttl) =>
        message.setTimeToLive(java.time.Duration.ofMillis(ttl))
      case _ =>
    }

    message
  }

  //create the topic is auto set, else check if it exist
  def initializeNamespaceEntities(client: ServiceBusAdministrationClient,
                                  name: String,
                                  targetType: servicebus.TargetType.TargetType,
                                  autoCreate: Boolean): Unit = {
    targetType match {
      case servicebus.TargetType.TOPIC =>
        if (!client.getTopicExists(name)) {
          if (autoCreate) {
            createTopic(client, name)
          } else {
            throw new ConnectException(
              s"Topic [$name] does not exist in namespace [${settings.namespace}]")
          }
        }

      case servicebus.TargetType.QUEUE =>
        if (!client.getQueueExists(name)) {
          if (autoCreate) {
            createQueue(client, name)
          } else {
            throw new ConnectException(
              s"Queue [$name] does not exist in namespace [${settings.namespace}]")
          }
        }
    }
  }

  // set the clients and set the topic or queue based on kcql
  def buildServiceBusClients(): Unit = {

    clients = settings.projections.targets.map {
      case (source, target) =>
        val connStr =
          s"Endpoint=sb://${settings.namespace}/;SharedAccessKeyName=${settings.sapName};SharedAccessKey=${settings.sapKey.value()}"

        val managementClient = new ServiceBusAdministrationClientBuilder()
          .connectionString(connStr)
          .buildClient()
        val senderClient = new ServiceBusClientBuilder()
          .connectionString(connStr)
          .sender()

        settings.targetType(source) match {
          case t @ servicebus.TargetType.TOPIC =>
            initializeNamespaceEntities(
              managementClient,
              target,
              t,
              settings.projections.autoCreate.getOrElse(source, false))

            // set the target topic name
            senderClient.topicName(target)

          case q @ servicebus.TargetType.QUEUE =>
            initializeNamespaceEntities(
              managementClient,
              target,
              q,
              settings.projections.autoCreate.getOrElse(source, false))

            // set the target queue name
            senderClient.queueName(target)

        }
        (source, senderClient.buildAsyncClient())
    }
  }

  private def createTopic(client: ServiceBusAdministrationClient,
                          name: String): Unit = {

    val topicOptions = new CreateTopicOptions()
      .setUserMetadata(classOf[AzureServiceBusSinkConnector].getCanonicalName)

    Try(client.createTopic(name, topicOptions)) match {
      case Success(_) =>
        logger.info(
          s"Created topic [$name] in namespace [${settings.namespace}]")
      case Failure(exception) =>
        throw new ConnectException(
          s"Failed to create topic [$name] in namespace [${settings.namespace}]",
          exception)
    }
  }

  private def createQueue(client: ServiceBusAdministrationClient,
                          name: String): Unit = {

    val queueOptions = new CreateQueueOptions()
      .setBatchedOperationsEnabled(true)
      .setDuplicateDetectionRequired(true)
      .setPartitioningEnabled(true)

    settings.projections.sessions.get(name) match {
      case Some(_) => queueOptions.setSessionRequired(true)
      case _       =>
    }

    Try(client.createQueue(name, queueOptions)) match {
      case Success(_) =>
        logger.info(
          s"Created queue [$name] in namespace [${settings.namespace}]")
      case Failure(exception) =>
        throw new ConnectException(
          s"Failed to create queue [$name] in namespace [${settings.namespace}]",
          exception)
    }

  }
}
