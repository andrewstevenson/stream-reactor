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

package io.lenses.streamreactor.connect.azure.servicebus.source

import io.lenses.streamreactor.connect.azure.servicebus.config.{AzureServiceBusConfig, AzureServiceBusSettings}
import io.lenses.streamreactor.connect.azure.servicebus.{TestBase, getConverters}
import org.apache.kafka.connect.source.SourceTaskContext

import scala.collection.JavaConverters._

class AzureServiceBusSourceTaskTest extends TestBase {

  "should process in a task" in {
    val props = getProps(s"INSERT INTO kafka-topic SELECT * FROM $QUEUE  STOREAS queue WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`")
    val conf = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(conf)
    val reader = AzureServiceBusReader(CONNECTOR_NAME, settings, getConverters(settings.converters, props.asScala.toMap))
    reader.clients = Map(QUEUE -> (AzureServiceBusConfig.DEFAULT_BATCH_SIZE, mockClient))
    reader.managementClient = mockManagementClient
    val task = new AzureServiceBusSourceTask
    val context = mock[SourceTaskContext]
    when(context.configs()).thenReturn(props)
    task.initialize(context)
    task.start(props)
    task.reader = reader
    val records = task.poll()
    task.getRecordsToCommit.size() shouldBe records.size()
    task.commitRecord(records.asScala.head)
    task.getRecordsToCommit.isEmpty shouldBe true
  }

}
