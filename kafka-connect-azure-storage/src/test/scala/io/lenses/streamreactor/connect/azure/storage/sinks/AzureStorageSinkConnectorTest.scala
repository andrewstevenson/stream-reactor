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

package io.lenses.streamreactor.connect.azure.storage.sinks

import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class AzureStorageSinkConnectorTest extends AnyWordSpec with Matchers {

  val props = Map(
    "topics" -> "topic",
    AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
    AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
    AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
    AzureStorageConfig.KCQL -> s"INSERT INTO table SELECT * FROM topic"
  )

  "should start a connector" in {
    val connector = new AzureStorageSinkConnector()
    connector.start(props.asJava)
    connector.taskClass() shouldBe classOf[AzureStorageSinkTask]
    connector.config() shouldBe AzureStorageConfig.config
    connector.taskConfigs(1).size() shouldBe 1
  }
}
