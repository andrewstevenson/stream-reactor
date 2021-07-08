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

import com.datamountaineer.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.azure.servicebus.config.{AzureServiceBusConfig, AzureServiceBusSettings}
import io.lenses.streamreactor.connect.azure.servicebus.{TestBase, _}

import scala.collection.JavaConverters._

class AzureServiceBusReaderTest extends TestBase with ConverterUtil {

  "should create a reader and read from a queue" in {
    val props = getProps(
      s"INSERT INTO kafka-topic SELECT * FROM $QUEUE  STOREAS queue WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`")
    val conf = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(conf)
    val reader = AzureServiceBusReader(CONNECTOR_NAME,
      settings,
      getConverters(settings.converters, props.asScala.toMap))
    reader.clients = Map(QUEUE -> (AzureServiceBusConfig.DEFAULT_BATCH_SIZE, mockClient))
    reader.managementClient = mockManagementClient
    val results = reader.read()
    results.head.source shouldBe QUEUE
    results.head.record.topic() shouldBe "kafka-topic"
  }

  "should create a reader and read from a topic" in {
    val props = getProps(
      s"INSERT INTO kafka-topic SELECT * FROM $TOPIC STOREAS TOPIC WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter` WITHSUBSCRIPTION = $CONNECTOR_NAME")
    val conf = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(conf)
    val reader = AzureServiceBusReader(CONNECTOR_NAME,
      settings,
      getConverters(settings.converters, props.asScala.toMap))
    reader.clients = Map(TOPIC -> (AzureServiceBusConfig.DEFAULT_BATCH_SIZE, mockClient))
    reader.managementClient = mockManagementClient
    val results = reader.read()
    results.head.source shouldBe TOPIC
    results.head.record.topic() shouldBe "kafka-topic"
    val headers = results.head.record.headers()
    headers.size() shouldBe 3
  }

  "should create a reader and read from a topic with session" in {
    val props = getProps(
      s"INSERT INTO kafka-topic SELECT * FROM $TOPIC STOREAS TOPIC WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter` WITHSUBSCRIPTION = $CONNECTOR_NAME WITHSESSION = lenses")
    val conf = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(conf)
    val reader = AzureServiceBusReader(CONNECTOR_NAME,
      settings,
      getConverters(settings.converters, props.asScala.toMap))
    reader.clients = Map(TOPIC -> (AzureServiceBusConfig.DEFAULT_BATCH_SIZE, mockClient))
    reader.managementClient = mockManagementClient
    val results = reader.read()
    results.head.source shouldBe TOPIC
    results.head.record.topic() shouldBe "kafka-topic"
    val headers = results.head.record.headers()
    headers.size() shouldBe 3
  }
}


