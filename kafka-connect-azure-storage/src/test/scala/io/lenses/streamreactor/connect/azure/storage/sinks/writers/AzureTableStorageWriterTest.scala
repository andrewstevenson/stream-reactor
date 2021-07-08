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

import com.microsoft.azure.storage.table._
import io.lenses.streamreactor.connect.azure.TestBase
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._

class AzureTableStorageWriterTest extends TestBase {

  val props = Map(
    AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
    AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
    AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
    AzureStorageConfig.KCQL -> s"INSERT INTO $TABLE SELECT * FROM $TOPIC AUTOCREATE STOREAS table"
  )

  val config: AzureStorageConfig = AzureStorageConfig(props.asJava)
  val settings: AzureStorageSettings = AzureStorageSettings(config)
  val tableClient: CloudTableClient = mock[CloudTableClient]
  val cloudTable: CloudTable = mock[CloudTable]
  when(cloudTable.exists()).thenReturn(true)
  when(tableClient.getTableReference(TABLE)).thenReturn(cloudTable)

  val records: Seq[SinkRecord] = getTestRecords(1)
  val struct: Struct = records.head.value().asInstanceOf[Struct]

  "should convert a schema to table storage edm type" in {
    val writer = new AzureTableStorageWriter(settings, tableClient)

    //int8
    val int8 = writer
      .entityFromFieldType(queueStruct.get("int8_field"),
                           queueStruct.schema().field("int8_field"))
      .get
    int8.getEdmType shouldBe EdmType.INT32
    int8.getValueAsInteger shouldBe 2

    //int32
    val int32 = writer
      .entityFromFieldType(queueStruct.get("int32_field"),
                           queueStruct.schema().field("int32_field"))
      .get
    int32.getEdmType shouldBe EdmType.INT32
    int32.getValueAsInteger shouldBe 12

    //int32 optional
    val int32o = writer
      .entityFromFieldType(queueStruct.get("int32_field_optional"),
                           queueStruct.schema().field("int32_field_optional"))
      .get
    int32o.getEdmType shouldBe EdmType.INT32
    int32o.getValueAsInteger shouldBe 1

    //int64 optional
    val int64o = writer
      .entityFromFieldType(queueStruct.get("int64_field_optional"),
                           queueStruct.schema().field("int64_field_optional"))
      .get
    int64o.getEdmType shouldBe EdmType.INT64
    int64o.getValueAsInteger shouldBe 1

    //string
    val string = writer
      .entityFromFieldType(queueStruct.get("string_field"),
                           queueStruct.schema().field("string_field"))
      .get
    string.getEdmType shouldBe EdmType.STRING
    string.getValueAsString shouldBe "foo"

    //boolean
    val boolean = writer
      .entityFromFieldType(queueStruct.get("boolean_field"),
                           queueStruct.schema().field("boolean_field"))
      .get
    boolean.getEdmType shouldBe EdmType.BOOLEAN
    boolean.getValueAsBoolean shouldBe true

    //float32
    val float32 = writer
      .entityFromFieldType(queueStruct.get("float32_field"),
                           queueStruct.schema().field("float32_field"))
      .get
    float32.getEdmType shouldBe EdmType.DOUBLE
    float32.getValueAsDoubleObject
      .floatValue()
      .toString shouldBe 1.1234567.toFloat.toString

    //float64
    val float64 = writer
      .entityFromFieldType(queueStruct.get("float64_field"),
                           queueStruct.schema().field("float64_field"))
      .get
    float64.getEdmType shouldBe EdmType.DOUBLE
    float64.getValueAsDouble shouldBe 1.123456789101112

    //bytes
    val bytes = writer
      .entityFromFieldType(queueStruct.get("byte_field"),
                           queueStruct.schema().field("byte_field"))
      .get
    bytes.getEdmType shouldBe EdmType.BINARY
    bytes.getValueAsByteArray shouldBe "bytes".getBytes
  }

  "check logical types" in {
    val writer = new AzureTableStorageWriter(settings, tableClient)

    val lDate = writer
      .entityFromLogicalType(queueStruct.get("logical_date"),
                             queueStruct.schema().field("logical_date"))
      .get
    lDate.getEdmType shouldBe EdmType.DATE_TIME
    lDate.getValueAsDate shouldBe DATE

    val lTime = writer
      .entityFromLogicalType(queueStruct.get("logical_time"),
                             queueStruct.schema().field("logical_time"))
      .get
    lTime.getEdmType shouldBe EdmType.DATE_TIME
    lTime.getValueAsDate shouldBe DATE

    val lTimestamp = writer
      .entityFromLogicalType(queueStruct.get("logical_timestamp"),
                             queueStruct.schema().field("logical_timestamp"))
      .get
    lTimestamp.getEdmType shouldBe EdmType.DATE_TIME
    lTimestamp.getValueAsDate shouldBe DATE
  }

  "should get a seq of entities" in {
    val writer = new AzureTableStorageWriter(settings, tableClient)
    val entities = writer.getEntityProperties(queueStruct)
    val dateEntity = entities.get("logical_date")
    dateEntity.getEdmType shouldBe EdmType.DATE_TIME
    dateEntity.getValueAsDate shouldBe DATE

    val stringEntity = entities.get("string_field")
    stringEntity.getEdmType shouldBe EdmType.STRING
    stringEntity.getValueAsString shouldBe "foo"

    entities.containsKey("int8_field_optional") shouldBe true
    //contain optional field with default
    new String(entities.get("byte_field_optional").getValueAsByteArray) shouldBe "bar"
    //not contain optional with no default
    entities.containsKey("int32_optional") shouldBe false

  }

  "should process records" in {
    val writer = new AzureTableStorageWriter(settings, tableClient)
    val entity = writer.processRecords(records.head)
    entity.getRowKey shouldBe s"$TOPIC-$PARTITION-1"
    entity.getPartitionKey shouldBe new SimpleDateFormat("YYYY-MM-dd")
      .format(new Date(records.head.timestamp()))
    entity.getProperties.size() shouldBe 20
  }

  "should process records with different date format" in {
    val oProps = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TABLE SELECT * FROM $TOPIC STOREAS table",
      AzureStorageConfig.PARTITION_DATE_FORMAT -> "YYYY-MM-dd HH:mm:ss"
    )
    val oConfig = AzureStorageConfig(oProps.asJava)

    val oSettings = AzureStorageSettings(oConfig)
    val writer = new AzureTableStorageWriter(oSettings, tableClient)
    val entity = writer.processRecords(records.head)
    entity.getRowKey shouldBe s"$TOPIC-$PARTITION-1"
    entity.getPartitionKey shouldBe new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
      .format(new Date(records.head.timestamp()))
    entity.getProperties.size() shouldBe 20
  }

  "should process records with fields for partitioning" in {
    val oProps = Map(
      AzureStorageConfig.AZURE_ACCOUNT -> "myaccount",
      AzureStorageConfig.AZURE_ACCOUNT_KEY -> "myaccountkey",
      AzureStorageConfig.AZURE_ENDPOINT -> "myendpoint",
      AzureStorageConfig.KCQL -> s"INSERT INTO $TABLE SELECT * FROM $TOPIC BATCH = 10 PARTITIONBY int32_field, string_field KEYDELIMITER = '|' STOREAS table",
      AzureStorageConfig.PARTITION_DATE_FORMAT -> "YYYY-MM-dd HH:mm:ss"
    )
    val oConfig = AzureStorageConfig(oProps.asJava)

    val oSettings = AzureStorageSettings(oConfig)
    val writer = new AzureTableStorageWriter(oSettings, tableClient)
    val entity = writer.processRecords(records.head)
    entity.getRowKey shouldBe s"$TOPIC-$PARTITION-1"
    entity.getPartitionKey shouldBe "12|foo"
    entity.getProperties.size() shouldBe 20
  }

  "should process batch" in {
    val writer = new AzureTableStorageWriter(settings, tableClient)
    writer.createBatch(records).isEmpty shouldBe false
  }

  "should write" in {
    val writer = AzureStorageWriter(settings)
    writer.writers =
      Map(TOPIC -> new AzureTableStorageWriter(settings, tableClient))
    writer.write(getTestRecords(10000))
  }
}
