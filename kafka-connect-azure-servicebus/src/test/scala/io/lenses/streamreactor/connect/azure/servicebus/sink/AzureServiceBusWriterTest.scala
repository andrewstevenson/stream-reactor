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

import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient
import com.azure.messaging.servicebus.{ServiceBusMessageBatch, ServiceBusSenderAsyncClient}
import io.lenses.streamreactor.connect.azure.servicebus.config.{AzureServiceBusConfig, AzureServiceBusSettings}
import io.lenses.streamreactor.connect.azure.servicebus.{TargetType, TestBase}
import org.apache.kafka.connect.errors.ConnectException
import reactor.core.publisher.Mono

class AzureServiceBusWriterTest extends TestBase {


  "write should convert sink record to service bus message with default partitionkey" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val sbMessage = writer.convertToServiceBusMessage(connectRecord)
    sbMessage.getPartitionKey shouldBe s"$TOPIC-${PARTITION}-1"
    sbMessage.getBody.toBytes.isEmpty shouldBe false
    sbMessage.getContentType shouldBe "application/json"
    sbMessage.getTimeToLive shouldBe null
  }

  "write should convert sink record to service bus message with partition keys" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field KEYDELIMITER = |")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val sbMessage = writer.convertToServiceBusMessage(connectRecord)
    sbMessage.getPartitionKey shouldBe "12|foo"
    sbMessage.getBody.toBytes.isEmpty shouldBe false
    sbMessage.getContentType shouldBe "application/json"
    sbMessage.getTimeToLive shouldBe null
  }

  "write should convert sink record to service bus message with partition keys with ttl" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = |")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val sbMessage = writer.convertToServiceBusMessage(connectRecord)
    sbMessage.getPartitionKey shouldBe "12|foo"
    sbMessage.getBody.toBytes.isEmpty shouldBe false
    sbMessage.getContentType shouldBe "application/json"
    sbMessage.getTimeToLive.toMillis shouldBe 100
  }

  "write should convert sink record to service bus message with session and with ttl" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC TTL=100 WITH_SESSION = mysession")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val sbMessage = writer.convertToServiceBusMessage(connectRecord)
    sbMessage.getPartitionKey shouldBe "mysession"
    sbMessage.getSessionId shouldBe "mysession"
    sbMessage.getBody.toBytes.isEmpty shouldBe false
    sbMessage.getContentType shouldBe "application/json"
    sbMessage.getTimeToLive.toMillis shouldBe 100
  }

  "should process records" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = |")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val sbMessage = writer.convertToServiceBusMessage(connectRecord)
    val client = mock[ServiceBusSenderAsyncClient]

    val batch = mock[ServiceBusMessageBatch]
    when(batch.tryAddMessage(sbMessage)).thenReturn(true)

    val mono = Mono.just(batch)
    when(client.createMessageBatch()).thenReturn(mono)
    when(client.sendMessages(batch)).thenReturn(Mono.empty())
    val clients = Map(TOPIC -> client)
    writer.clients = clients
    writer.write(Seq(connectRecord))
  }

  "should create topics" in {
    val props = getProps(s"INSERT INTO sb_topic SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = |")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val client = mock[ServiceBusAdministrationClient]
    writer.initializeNamespaceEntities(client, "sb_topic", TargetType.TOPIC, autoCreate = true)
  }

  "should create queue" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = | STOREAS QUEUE")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val client = mock[ServiceBusAdministrationClient]
    writer.initializeNamespaceEntities(client, "sb_queue", TargetType.QUEUE, autoCreate = true)
  }

  "should fail to lookup topic" in {
    val props = getProps(s"INSERT INTO sb_topic SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = |")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val client = mock[ServiceBusAdministrationClient]
    an [ConnectException] shouldBe thrownBy(writer.initializeNamespaceEntities(client, "sb_topic", TargetType.TOPIC, autoCreate = false))
  }

  "should fail to lookup queue" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = | STOREAS QUEUE")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val client = mock[ServiceBusAdministrationClient]
    an [ConnectException] shouldBe thrownBy(writer.initializeNamespaceEntities(client, "sb_queue", TargetType.QUEUE, autoCreate = false))
  }

  "should lookup topic" in {
    val props = getProps(s"INSERT INTO sb_topic SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = |")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val client = mock[ServiceBusAdministrationClient]
    when(client.getTopicExists("sb_topic")).thenReturn(true)
    noException shouldBe thrownBy(writer.initializeNamespaceEntities(client, "sb_topic", TargetType.TOPIC, autoCreate = false))
  }

  "should lookup queue" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field TTL=100 KEYDELIMITER = |  STOREAS QUEUE")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    val writer = AzureServiceBusWriter(settings)
    val client = mock[ServiceBusAdministrationClient]
    when(client.getQueueExists("sb_queue")).thenReturn(true)
    noException shouldBe thrownBy(writer.initializeNamespaceEntities(client, "sb_queue", TargetType.QUEUE, autoCreate = false))
  }
}
