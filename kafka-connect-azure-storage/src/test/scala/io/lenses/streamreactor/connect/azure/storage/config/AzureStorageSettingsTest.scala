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

package io.lenses.streamreactor.connect.azure.storage.config

import com.datamountaineer.kcql.{FormatType, WriteModeEnum}
import io.lenses.streamreactor.connect.azure.TestBase

import scala.collection.JavaConverters._

class AzureStorageSettingsTest extends TestBase {
  "should set a basic setting for table storage" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> "INSERT INTO table1 SELECT * FROM mytopic"
    )

    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    settings.account shouldBe "myaccount"
    settings.accountKey.value() shouldBe "myaccountkey"
    settings.endpoint.get shouldBe "myendpoint"
    settings.datePartitionFormat shouldBe AzureStorageConfig.PARTITION_DATE_FORMAT_DEFAULT
  }

  "should set a setting for table storage with custom date format" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> "INSERT INTO table1 SELECT * FROM mytopic",
      AzureStorageConfig.PARTITION_DATE_FORMAT -> "YYYY-MM-DD HH"
    )

    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    settings.account shouldBe "myaccount"
    settings.accountKey.value() shouldBe "myaccountkey"
    settings.endpoint.get shouldBe "myendpoint"
    settings.datePartitionFormat shouldBe "YYYY-MM-DD HH"
  }

  "should set a setting for table storage with custom partition by" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> "INSERT INTO table1 SELECT * FROM mytopic",
      AzureStorageConfig.PARTITION_DATE_FORMAT -> "YYYY-MM-DD HH"
    )

    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    settings.account shouldBe "myaccount"
    settings.accountKey.value() shouldBe "myaccountkey"
    settings.endpoint.get shouldBe "myendpoint"
    settings.datePartitionFormat shouldBe "YYYY-MM-DD HH"
  }

  "should set a setting for table storage with keys" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> "INSERT INTO table1 SELECT * FROM mytopic PARTITIONBY f1,f2"
    )

    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    settings.account shouldBe "myaccount"
    settings.accountKey.value() shouldBe "myaccountkey"
    settings.endpoint.get shouldBe "myendpoint"
    settings.projections.partitionBy("mytopic").contains("f1") shouldBe true
    settings.projections.partitionBy("mytopic").contains("f2") shouldBe true
    settings.projections.writeMode("mytopic") shouldBe WriteModeEnum.INSERT
  }

  "should set a setting for table storage with upsert and batching" in {
    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> "UPSERT INTO table1 SELECT * FROM mytopic BATCH= 100 PARTITIONBY f1,f2 WITHFORMAT JSON"
    )

    //when(cloudQueue.retrieveMessages(AzureStorageConfig.QUEUE_SOURCE_MAX_BATCH_SIZE, AzureStorageConfig.DEFAULT_LOCK, null, null)).thenReturn(Iterable(queueMessage).asJava)
    val config = AzureStorageConfig(props.asJava)
    val settings = AzureStorageSettings(config)
    settings.projections.writeMode("mytopic") shouldBe WriteModeEnum.UPSERT
    settings.projections.batchSize("mytopic") shouldBe 100
    settings.projections.formats("mytopic") shouldBe FormatType.JSON
  }
}
