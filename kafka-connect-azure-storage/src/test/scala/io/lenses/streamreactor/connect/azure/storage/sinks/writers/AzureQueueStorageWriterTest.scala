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

package io.lenses.streamreactor.connect.azure.storage.sinks.writers

import io.lenses.streamreactor.connect.azure.TestBase
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}

import scala.collection.JavaConverters._

class AzureQueueStorageWriterTest extends TestBase {

  val props = Map(
    AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
    AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
    AzureStorageConfig.KCQL -> s"INSERT INTO $queue SELECT * FROM $TOPIC  WITHFORMAT JSON STOREAS queue"
  )

  "should convert to json" in {
    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    val writer = AzureQueueStorageWriter(settings, cloudQueueClient)
    writer.toJson(queueRecord)
  }

  "should create a message as json" in {
    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    val writer = AzureQueueStorageWriter(settings, cloudQueueClient)
    val cloudMessageResult = writer.convert(queueRecord)
    cloudMessageResult.getMessageContentAsString.isEmpty shouldBe false
  }

  "should create a message as binary" in {

    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.KCQL -> s"INSERT INTO $queue SELECT * FROM $TOPIC WITHFORMAT BINARY STOREAS queue"
    )

    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    val writer = AzureQueueStorageWriter(settings, cloudQueueClient)
    val cloudMessageResult = writer.convert(queueRecord)
    cloudMessageResult.getMessageContentAsByte.isEmpty shouldBe false
  }

  "should write a message as json" in {
    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    val writer = AzureQueueStorageWriter(settings, cloudQueueClient)
    writer.write(queue, Seq(queueRecord))
  }
}
