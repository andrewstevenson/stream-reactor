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

package io.lenses.streamreactor.connect.azure.storage.sources

import io.lenses.streamreactor.connect.azure.TestBase
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}
import io.lenses.streamreactor.connect.azure.storage.getConverters
import org.apache.kafka.connect.data.Struct

import scala.collection.JavaConverters._

class AzureQueueStorageStorageTaskTest extends TestBase {
  "should read from a queue as json with ack" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TOPIC SELECT * FROM $queue BATCH = 100 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter` WITH_ACK"
    ).asJava

    val settings = AzureStorageSettings(AzureStorageConfig(props))
    when(cloudQueue.retrieveMessages(100, AzureStorageConfig.DEFAULT_LOCK, null, null)).thenReturn(Iterable(queueMessage).asJava)

    val reader = AzureQueueStorageReader(
      "my-connector",
      settings,
      cloudQueueClient,
      getConverters(settings.converters, props.asScala.toMap))

    val task = new AzureQueueStorageSourceTask()
    task.reader = reader
    val result = task.poll().asScala.head
    val struct = result.value().asInstanceOf[Struct]

    val resultJson = jsonConverter.fromConnectData(struct.schema(), struct)
    resultJson.toString shouldBe queueJson.toString

    task.getRecordsToCommit.size() shouldBe 1
    task.commitRecord(result)
    task.getRecordsToCommit.size() shouldBe 0
  }

  "should read from a queue as json with no ack" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TOPIC SELECT * FROM $queue BATCH = 100 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`"
    ).asJava

    when(cloudQueue.retrieveMessages(100, AzureStorageConfig.DEFAULT_LOCK, null, null)).thenReturn(Iterable(queueMessage).asJava)

    val settings = AzureStorageSettings(AzureStorageConfig(props))
    val reader = AzureQueueStorageReader(
      "my-connector",
      settings,
      cloudQueueClient,
      getConverters(settings.converters, props.asScala.toMap))


    val task = new AzureQueueStorageSourceTask()
    task.reader = reader
    val result = task.poll().asScala.head
    val struct = result.value.asInstanceOf[Struct]

    val resultJson = jsonConverter.fromConnectData(struct.schema(), struct)
    resultJson.toString shouldBe queueJson.toString

    task.getRecordsToCommit.size() shouldBe 0
    task.commitRecord(result)
    task.getRecordsToCommit.size() shouldBe 0
  }
}
