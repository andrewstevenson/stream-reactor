/*
 *
 *  * Copyright 2020 lensesio
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

import com.azure.messaging.servicebus._
import com.azure.messaging.servicebus.administration.models.CreateSubscriptionOptions
import com.azure.messaging.servicebus.administration.{ServiceBusAdministrationClient, ServiceBusAdministrationClientBuilder}
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.servicebus
import io.lenses.streamreactor.connect.azure.servicebus.config.{AzureServiceBusConfig, AzureServiceBusSettings}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object AzureServiceBusReader {
  def apply(name: String = "",
            settings: AzureServiceBusSettings,
            convertersMap: Map[String, Converter]): AzureServiceBusReader =
    new AzureServiceBusReader(name,
                              settings,
                              convertersMap)
}

case class TokenAndSourceRecord(message: ServiceBusReceivedMessage,
                                source: String,
                                record: SourceRecord)

class AzureServiceBusReader(name: String = "",
                            settings: AzureServiceBusSettings,
                            convertersMap: Map[String, Converter])
    extends StrictLogging {

  var clients: Map[String, (Int, ServiceBusReceiverClient)] =
    Map.empty

  var managementClient: ServiceBusAdministrationClient = _

  def close(): Unit =
    clients.values.foreach({ case (_, client) => client.close() })

  // complete the service bus message
  def complete(source: String, message: ServiceBusReceivedMessage): Unit = {
    val (_, client) = clients(source)
    if (settings.projections.acks(source)) {
      client.complete(message)
    }
  }

  // read service bus message, convert and return the lock token and converted record
  def read(): List[TokenAndSourceRecord] = {

    // if the clients aren't set initialize them
    if (clients.isEmpty) buildClients()

    clients.flatMap {
      case (source, (batch, client)) =>
        readMessages(client, source, batch)
    }.toList
  }

  def readMessages(client: ServiceBusReceiverClient,
                   source: String,
                   batch: Int): List[TokenAndSourceRecord] = {

    client
      .peekMessages(batch)
      .iterator()
      .asScala
      .map(m => {
        TokenAndSourceRecord(
          m,
          source,
          convertServiceBusMessage(source, settings.projections.targets(source), m))
      })
      .toList
  }

  // convert the service bus message to a source record
  private def convertServiceBusMessage(
      source: String,
      target: String,
      message: ServiceBusReceivedMessage): SourceRecord = {

    val partitionKey = Option(message.getPartitionKey)
    val id = Option(message.getMessageId)

    val converter = convertersMap.getOrElse(
      source,
      throw new RuntimeException(
        s"[$source] converter is not defined in [${AzureServiceBusConfig.KCQL}]"))

    val headers = new ConnectHeaders()

    if (settings.setHeaders) {
      Option(message.getSessionId)
        .filter(s => s.nonEmpty)
        .foreach(s => headers.addString("session", s))
      Option(message.getContentType)
        .filter(s => s.nonEmpty)
        .foreach(l => headers.addString("contentType", l))
      Option(message.getCorrelationId)
        .filter(s => s.nonEmpty)
        .foreach(c => headers.addString("correlationId", c))
      Option(message.getDeliveryCount)
        .filter(_ > 0)
        .foreach(c => headers.addString("deliveryCount", c.toString))
      Option(message.getSequenceNumber)
        .filter(_ > 0)
        .foreach(s => headers.addString("sequenceNumber", s.toString))

      message
        .getApplicationProperties
        .asScala
        .map { case (k, v) => headers.addString(k, v.toString) }

    }

    val sourceRecord = converter.convert(target,
                                         source,
                                         partitionKey.getOrElse(id.orNull),
                                         message.getBody.toBytes,
                                         settings.projections.keys(source),
                                         settings.projections.keyDelimiters(source))

    sourceRecord.newRecord(
      sourceRecord.topic(),
      sourceRecord.kafkaPartition(),
      sourceRecord.keySchema(),
      sourceRecord.key(),
      sourceRecord.valueSchema(),
      sourceRecord.value(),
      sourceRecord.timestamp(),
      headers
    )
  }

  private def createEntities(connStr: String, source: String): Unit = {

    val subscriptionName = getSubscriptionName(source)
    managementClient = new ServiceBusAdministrationClientBuilder()
      .connectionString(connStr)
      .buildClient()

    settings.targetType(source) match {
      case servicebus.TargetType.TOPIC =>
        if (!managementClient.getTopicExists(source)) {
          throw new ConnectException(
            s"Topic [$name] does not exist in namespace [${settings.namespace}]")
        }

        //check for a subscription or make one with the connector name
        if (!managementClient.getSubscriptionExists(source, subscriptionName)) {
          val options = new CreateSubscriptionOptions()
            .setMaxDeliveryCount(settings.projections.batchSize(source))

          Try(
            managementClient
              .createSubscription(source, subscriptionName, options)) match {
            case Success(_) =>
              logger.info(
                s"Created subscription [$subscriptionName] on topic [$source] with MaxDeliveryCount [${settings.projections
                  .batchSize(source)}] in namespace [${settings.namespace}]")
            case Failure(e) =>
              throw new ConnectException(
                s"Failed to create subscription [$subscriptionName] in namespace [${settings.namespace}]",
                e)
          }
        }

      case servicebus.TargetType.QUEUE =>
        if (!managementClient.getQueueExists(source)) {
          throw new ConnectException(
            s"Queue [$name] does not exist in namespace [${settings.namespace}]")
        }

    }
  }

  private def getSubscriptionName(source: String): String = {
    Option(settings.projections.subscriptions.getOrElse(source, name)) match {
      case Some(sn) => sn
      case None     => name
    }
  }

  // set the clients and set the topic or queue based on kcql
  private def buildClients(): Unit = {

    val connStr =
      s"Endpoint=sb://${settings.namespace}/;SharedAccessKeyName=${settings.sapName};SharedAccessKey=${settings.sapKey.value()}"

    clients = settings.projections.targets.map {
      case (source, target) =>
        createEntities(connStr, source)

        val subscriptionName =
          Option(settings.projections.subscriptions.getOrElse(source, name)) match {
            case Some(sn) => sn
            case None     => name
          }

        val client = settings.projections.sessions.get(source) match {
          case Some(session) =>
            settings.targetType(source) match {
              case servicebus.TargetType.TOPIC =>
                new ServiceBusClientBuilder()
                  .connectionString(connStr)
                  .sessionReceiver()
                  .topicName(source)
                  .subscriptionName(subscriptionName)
                  .buildClient()
                  .acceptSession(session)

              case servicebus.TargetType.QUEUE =>
                new ServiceBusClientBuilder()
                  .connectionString(connStr)
                  .sessionReceiver()
                  .queueName(source)
                  .buildClient()
                  .acceptSession(session)

            }

          case None =>
            settings.targetType(source) match {
              case servicebus.TargetType.TOPIC =>
                new ServiceBusClientBuilder()
                  .connectionString(connStr)
                  .receiver()
                  .topicName(source)
                  .subscriptionName(subscriptionName)
                  .buildClient()

              case servicebus.TargetType.QUEUE =>
                new ServiceBusClientBuilder()
                  .connectionString(connStr)
                  .receiver()
                  .queueName(source)
                  .buildClient()
            }
        }

        (source, (settings.projections.batchSize(source), client))
    }
  }
}
