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
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageConfig

import scala.collection.JavaConverters._

class AzureQueueStorageSourceConnectorTest extends TestBase {
  "should start and configure a connector" in {

    val kcql1 = s"INSERT INTO TOPIC1 SELECT * FROM queue1 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`"
    val kcql2 = s"INSERT INTO TOPIC2 SELECT * FROM queue2 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`"

    val props = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"$kcql1;$kcql2"
    ).asJava

    val connector = new AzureQueueStorageSourceConnector()
    connector.start(props)
    val taskConfig = connector.taskConfigs(2)
    taskConfig.size() shouldBe 2
    taskConfig.asScala.head.get(AzureStorageConfig.KCQL) shouldBe kcql1
    taskConfig.asScala.last.get(AzureStorageConfig.KCQL) shouldBe kcql2
  }

}
