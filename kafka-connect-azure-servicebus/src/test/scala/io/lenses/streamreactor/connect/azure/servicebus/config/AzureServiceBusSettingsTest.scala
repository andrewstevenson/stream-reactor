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

package io.lenses.streamreactor.connect.azure.servicebus.config


import io.lenses.streamreactor.connect.azure.servicebus.{TargetType, TestBase}

class AzureServiceBusSettingsTest extends TestBase {


  "should create a settings with credentials" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    settings.namespace shouldBe "mynamespace"
    settings.projections.ttl.isEmpty shouldBe true
    settings.projections.valueFields.size shouldBe 1
    settings.targetType(TOPIC) shouldBe TargetType.TOPIC
  }

  "should set ttl" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC TTL=100")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    settings.projections.ttl(TOPIC) shouldBe 100
  }

  "should set storedas to queue" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC STOREAS QUEUE TTL=100")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    settings.projections.ttl(TOPIC) shouldBe 100
    settings.targetType(TOPIC) shouldBe TargetType.QUEUE
  }

  "should set primary keys" in {
    val props = getProps(s"INSERT INTO sb_queue SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field STOREAS QUEUE TTL=100")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    settings.projections.valueFields(TOPIC).contains("int32_field")
    settings.projections.valueFields(TOPIC).contains("string_field")
    settings.projections.ttl(TOPIC) shouldBe 100
    settings.targetType(TOPIC) shouldBe TargetType.QUEUE
  }

  "should set subscription" in {
    val props = getProps(s"INSERT INTO sb_topic SELECT * FROM $TOPIC PARTITIONBY int32_field, string_field STOREAS TOPIC TTL=100 WITHSUBSCRIPTION = lenses")
    val config = AzureServiceBusConfig(props)
    val settings = AzureServiceBusSettings(config)
    settings.projections.valueFields(TOPIC).contains("int32_field")
    settings.projections.valueFields(TOPIC).contains("string_field")
    settings.projections.ttl(TOPIC) shouldBe 100
    settings.targetType(TOPIC) shouldBe TargetType.TOPIC
    settings.projections.subscriptions(TOPIC) shouldBe "lenses"
  }
}
