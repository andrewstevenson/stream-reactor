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

import com.microsoft.azure.storage.table.{CloudTable, CloudTableClient}
import io.lenses.streamreactor.connect.azure.TestBase
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageConfig
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar

import scala.collection.JavaConverters._

class AzureStorageSinkTaskTest extends TestBase with MockitoSugar {

  val kcql1 = s"INSERT INTO TABLE1 SELECT * FROM $TOPIC"
  val kcql2 = s"INSERT INTO TABLE2 SELECT * FROM $TOPIC StoreAs QUEUE"

  val props = Map(
    AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
    AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
    AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
    AzureStorageConfig.KCQL -> s"$kcql1;$kcql2"
  )

  "should create and start a task" in {

    val tableClient: CloudTableClient = mock[CloudTableClient]
    val cloudTable: CloudTable = mock[CloudTable]
    when(tableClient.getTableReference(TABLE)).thenReturn(cloudTable)

    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    when(context.configs()).thenReturn(props.asJava)

    val task = new AzureStorageSinkTask()
    task.initialize(context)
    task.start(props.asJava)
    task.stop()
   }
}
