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

package io.lenses.streamreactor.connect.azure.storage.sources

import io.lenses.streamreactor.connect.azure.TestBase
import io.lenses.streamreactor.connect.azure.storage._
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}
import org.apache.kafka.connect.data.Struct

import scala.collection.JavaConverters._

class AzureQueueStorageReaderTest extends TestBase {

  "should read from a queue as json" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TOPIC SELECT * FROM lenses-demo BATCH = 100 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`"
    ).asJava

    val settings = AzureStorageSettings(AzureStorageConfig(props))
    when(cloudQueue.retrieveMessages(100, AzureStorageConfig.DEFAULT_LOCK, null, null)).thenReturn(Iterable(queueMessage).asJava)

    val reader = AzureQueueStorageReader(
      "my-connector",
      settings,
      cloudQueueClient,
      getConverters(settings.converters, props.asScala.toMap))
    val result = reader.read().head
    val struct = result.record.value().asInstanceOf[Struct]

    val resultJson = jsonConverter.fromConnectData(struct.schema(), struct)
    resultJson.toString shouldBe queueJson.toString
  }

  "should read from a queue as json with keys" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TOPIC SELECT * FROM $queue BATCH = 100 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter` WITHKEY(id, string_field) DELIMITER='.'"
    ).asJava

    val settings = AzureStorageSettings(AzureStorageConfig(props))

    when(cloudQueue.retrieveMessages(100, AzureStorageConfig.DEFAULT_LOCK, null, null)).thenReturn(Iterable(queueMessage).asJava)

    val reader = AzureQueueStorageReader(
      "my-connector",
      settings,
      cloudQueueClient,
      getConverters(settings.converters, props.asScala.toMap))

    val result = reader.read().head.record
    result.key().toString shouldBe s"${queueStruct.getString("id")}.${queueStruct.getString("string_field")}"
    val struct = result.value().asInstanceOf[Struct]

    val jsonResult = jsonConverter.fromConnectData(struct.schema(), struct)
    jsonResult.toString shouldBe queueJson.toString
  }

  "should read from a queue as json and ack" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TOPIC SELECT * FROM $queue BATCH = 100 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter` WITH_ACK",
      AzureStorageConfig.SET_HEADERS -> "true"
    ).asJava

    when(cloudQueue.retrieveMessages(100, AzureStorageConfig.DEFAULT_LOCK, null, null)).thenReturn(Iterable(queueMessage).asJava)

    val settings = AzureStorageSettings(AzureStorageConfig(props))
    val reader = AzureQueueStorageReader(
      "my-connector",
      settings,
      cloudQueueClient,
      getConverters(settings.converters, props.asScala.toMap))
    val result: MessageAndSourceRecord = reader.read().head
    val struct = result.record.value().asInstanceOf[Struct]

    val resultJson = jsonConverter.fromConnectData(struct.schema(), struct)
    resultJson.toString shouldBe queueJson.toString
    result.ack shouldBe true

    val headers = result.record.headers()
    headers.allWithName(AzureStorageConfig.HEADER_REMOVED).asScala.toList.head.value() shouldBe true
    headers.allWithName(AzureStorageConfig.HEADER_MESSAGE_ID).asScala.toList.head.value() shouldBe "1"
  }
}
