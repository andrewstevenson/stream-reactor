/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.schemas

import com.datamountaineer.streamreactor.common.TestUtilsBase
import com.datamountaineer.streamreactor.common.config.base.settings.Projections
import com.datamountaineer.streamreactor.common.errors.{ErrorPolicy, ErrorPolicyEnum}
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 29/02/16.
  * stream-reactor
  */
class TestConverterUtil extends TestUtilsBase with ConverterUtil {

  "ConverterUtil" should {
    "convert a SinkRecord Value to json" in {
      val testRecord = getTestRecord
      val json = convertValueToJson(testRecord).toString
      json shouldBe VALUE_JSON_STRING
    }

    "convert a SinkRecord Key to json" in {
      val testRecord = getTestRecord
      val json = convertKeyToJson(testRecord).asText()
      json shouldBe KEY
    }

    "convert a SinkRecord Key to avro" in {
      val testRecord = getTestRecord
      val avro = convertValueToGenericAvro(testRecord)
      val testAvro = buildAvro()
      avro.get("id") shouldBe testAvro.get("id")
      avro.get("int_field") shouldBe testAvro.get("int_field")
      avro.get("long_field") shouldBe testAvro.get("long_field")
      avro.get("string_field") shouldBe testAvro.get("string_field")
    }

    "return a subset SinkRecord" in {
      val testRecord = getTestRecord
      val converted =
        convert(testRecord, Map("id" -> "id", "int_field" -> "int_field"))
      val fields =
        converted.valueSchema().fields().asScala.map(f => f.name()).toSet
      fields.contains("id") shouldBe true
      fields.contains("int_field") shouldBe true
      fields.contains("long_field") shouldBe false
    }

    "handle nested fields" in {
      val avroData = new AvroData(1)
      val avro = buildNestedAvro()
      val testRecord = avroData.toConnectData(avro.getSchema, avro)
      val input = new SinkRecord(TOPIC,
                                 1,
                                 Schema.STRING_SCHEMA,
                                 KEY,
                                 testRecord.schema(),
                                 testRecord.value(),
                                 1)
      val converted = convert(input, Map("x" -> "x", "y.a" -> "a"))
      val fields =
        converted.valueSchema().fields().asScala.map(f => f.name()).toSet
      fields.contains("x") shouldBe true
      fields.contains("a") shouldBe true
      fields.contains("long_field") shouldBe false
      converted.value().asInstanceOf[Struct].get("x") shouldBe 1.1
      converted.value().asInstanceOf[Struct].get("a") shouldBe "abc"
    }

    "return a ignore fields SinkRecord" in {
      val testRecord = getTestRecord
      val converted =
        convert(testRecord, Map.empty[String, String], Set("long_field"))
      val fields =
        converted.valueSchema().fields().asScala.map(f => f.name()).toSet
      fields.contains("long_field") shouldBe false
    }

    "return same SinkRecord" in {
      val testRecord = getTestRecord
      val converted =
        convert(testRecord, Map.empty[String, String], Set.empty[String])
      converted shouldBe testRecord
    }

    "throw an error while converting schemaless record if the payload is not Map[String, Any]" in {
      intercept[RuntimeException] {
        val record =
          new SinkRecord("t", 0, null, null, null, "Should not be here", 0)
        val projections = new Projections(
          writeMode = Map.empty,
          targets = Map.empty,
          headerFields = Map.empty,
          keyFields = Map.empty,
          valueFields = Map("t" -> Map("*" -> "*")),
          ignoreFields = Map.empty,
          primaryKeys = Map.empty,
          ttl = Map.empty,
          batchSize = Map.empty,
          autoCreate = Map.empty,
          autoEvolve = Map.empty,
          partitionBy = Map.empty,
          formats = Map.empty,
          tags = Map.empty,
          withType = Map.empty,
          dynamicTarget = Map.empty,
          keyDelimiters = Map.empty,
          subscriptions = Map.empty,
          sessions = Map.empty,
          keys = Map.empty,
          acks = Map.empty,
          errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
          errorRetries = 0,
          kcqls = Set.empty
        )
        record.newFilteredRecordAsStruct(projections)
      }
    }

    "remove the specified field when converting a schemaless record" in {
      val map = new util.HashMap[String, Any]()
      map.put("field1", "value1")
      map.put("field2", 3)
      map.put("toremove", null)

      val record = new SinkRecord("t", 0, null, null, null, map, 0)

      val projections =
        new Projections(
          targets = Map.empty,
          headerFields = Map.empty,
          keyFields = Map.empty,
          valueFields = Map("t" -> Map("*" -> "*")),
          ignoreFields = Map("t" -> Set("toremove")),
          primaryKeys = Map.empty,
          writeMode = Map.empty,
          ttl = Map.empty,
          batchSize = Map.empty,
          autoCreate = Map.empty,
          autoEvolve = Map.empty,
          partitionBy = Map.empty,
          formats = Map.empty,
          tags = Map.empty,
          withType = Map.empty,
          dynamicTarget = Map.empty,
          keyDelimiters = Map.empty,
          subscriptions = Map.empty,
          sessions = Map.empty,
          keys = Map.empty,
          acks = Map.empty,
          errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
          errorRetries = 0,
          kcqls = Set.empty
        )
      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"field1\":\"value1\",\"field2\":3}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "only select the fields specified when converting a schemaless sink with the value being a json" in {
      val map = new util.HashMap[String, Any]()
      map.put("field1", "value1")
      map.put("field2", 3)
      map.put("field3", null)

      val record = new SinkRecord("t", 0, null, null, null, map, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields =
          Map("t" -> Map("field1" -> "field1", "field2" -> "fieldRenamed")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"field1\":\"value1\",\"fieldRenamed\":3}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "rename the specified field when converting a schemaless record" in {
      val map = new util.HashMap[String, Any]()
      map.put("field1", "value1")
      map.put("field2", 3)
      map.put("field3", null)

      val record = new SinkRecord("t", 0, null, null, null, map, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(
          "t" -> Map("field1" -> "field1",
                     "field2" -> "fieldRenamed",
                     "field3" -> "field3")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"field1\":\"value1\",\"fieldRenamed\":3,\"field3\":null}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "convert a json via JsonConverter and then apply a field alias and one remove " in {
      val converter = new JsonConverter()
      converter.configure(Map("schemas.enable" -> false).asJava, false)

      val schemaAndValue = converter.toConnectData(
        "topicA",
        """
          |{
          |    "id": 1,
          |    "name": "A green door",
          |    "price": 12.50,
          |    "tags": ["home", "green"]
          |}
        """.stripMargin.getBytes
      )

      val map =
        schemaAndValue.value().asInstanceOf[java.util.Map[String, Any]].asScala
      map shouldBe Map("id" -> 1,
                       "name" -> "A green door",
                       "price" -> 12.5,
                       "tags" -> List("home", "green").asJava)

      val record =
        new SinkRecord("t", 0, null, null, null, schemaAndValue.value, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map("t" -> Map("id" -> "id", "tags" -> "tagsRenamed")),
        ignoreFields = Map("t" -> Set("price")),
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"id\":1,\"tagsRenamed\":[\"home\",\"green\"]}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "throw an error while converting a json payload" in {
      intercept[RuntimeException] {
        val record =
          new SinkRecord("t", 0, null, null, null, Map.empty[String, String], 0)

        val projections = new Projections(
          targets = Map.empty,
          headerFields = Map.empty,
          keyFields = Map.empty,
          valueFields = Map("t" -> Map("id" -> "id", "tags" -> "tagsRenamed")),
          ignoreFields = Map("t" -> Set("price")),
          primaryKeys = Map.empty,
          writeMode = Map.empty,
          ttl = Map.empty,
          batchSize = Map.empty,
          autoCreate = Map.empty,
          autoEvolve = Map.empty,
          partitionBy = Map.empty,
          formats = Map.empty,
          tags = Map.empty,
          withType = Map.empty,
          dynamicTarget = Map.empty,
          keyDelimiters = Map.empty,
          subscriptions = Map.empty,
          sessions = Map.empty,
          keys = Map.empty,
          acks = Map.empty,
          errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
          errorRetries = 0,
          kcqls = Set.empty
        )

        record.newFilteredRecordAsStruct(projections)
      }
    }

    "remove the specified field when converting a json for a record with Schema.String" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "toremove":""
          |}
        """.stripMargin

      val record =
        new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map("t" -> Map("*" -> "*")),
        ignoreFields = Map("t" -> Set("toremove")),
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"field1\":\"value1\",\"field2\":3}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "only select the fields specified when converting a record with Schema.String and payload a json string" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "field3":""
          |}
        """.stripMargin

      val record =
        new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields =
          Map("t" -> Map("field1" -> "field1", "field2" -> "fieldRenamed")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"field1\":\"value1\",\"fieldRenamed\":3}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "rename the specified field when converting a record with Schema.String and value is json" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "field3":""
          |}
        """.stripMargin

      val record =
        new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields =
          Map("t" -> Map("field1" -> "field1", "field2" -> "fieldRenamed")),
        ignoreFields = Map("t" -> Set("toremove")),
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"field1\":\"value1\",\"fieldRenamed\":3}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "apply a field alias and one remove when converting a sink record with Schema.String and the payload a json" in {
      val json =
        """
          |{
          |    "id": 1,
          |    "name": "A green door",
          |    "price": 12.50,
          |    "tags": ["home", "green"]
          |}
        """.stripMargin

      val record =
        new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map("t" -> Map("id" -> "id", "tags" -> "tagsRenamed")),
        ignoreFields = Map("t" -> Set("price")),
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"id\":1,\"tagsRenamed\":[\"home\",\"green\"]}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "add key fields and headers to record for Struct" in {
      val originalRecord = sinkRecordWithKeyHeaders()
      val expected =
        "{\"key_int_field\":1,\"int_field\":1,\"header_alias_field_3\":\"boo\"}"

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map(
          originalRecord.topic() -> Map(
            "header_field_3" -> "header_alias_field_3")),
        keyFields = Map(
          originalRecord.topic() -> Map("key_int_field" -> "key_int_field")),
        valueFields =
          Map(originalRecord.topic() -> Map("int_field" -> "int_field")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      val recordFields = combinedRecord
        .schema()
        .fields()
        .asScala
        .map(_.name())

      recordFields.contains("key_int_field") shouldBe true
      recordFields.contains("header_alias_field_3") shouldBe true
      recordFields.size shouldBe 3

      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should not add key and header fields for struct" in {

      val originalRecord = sinkRecordWithKeyHeaders()

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(
          originalRecord.topic() -> createSchema
            .fields()
            .asScala
            .map(f => (f.name(), f.name()))
            .toMap),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      val recordFields = combinedRecord
        .schema()
        .fields()
        .asScala
        .map(_.name())

      recordFields.contains("key_int_field") shouldBe false
      recordFields.contains("header_alias_field_3") shouldBe false

      recordFields.size shouldBe originalRecord
        .valueSchema()
        .fields()
        .size
    }

    "select * from value fields for struct" in {

      val originalRecord = getTestRecord
      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(originalRecord.topic() -> Map("*" -> "*")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      combinedRecord.schema().fields().asScala.size shouldBe originalRecord
        .valueSchema()
        .fields()
        .size
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe (
        simpleJsonConverter
          .fromConnectData(originalRecord.valueSchema(), originalRecord.value())
          .toString
      )
    }

    "select * from key fields for struct" in {

      val originalRecord = sinkRecordWithKeyHeaders()

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map(originalRecord.topic() -> Map("*" -> "*")),
        valueFields = Map.empty,
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      combinedRecord.schema().fields().asScala.size shouldBe originalRecord
        .valueSchema()
        .fields()
        .size
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe (
        simpleJsonConverter
          .fromConnectData(originalRecord.keySchema(), originalRecord.key())
          .toString
      )
    }

    "select * from headers fields for struct" in {

      val originalRecord = sinkRecordWithKeyHeaders()

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map(originalRecord.topic() -> Map("*" -> "*")),
        keyFields = Map.empty,
        valueFields = Map.empty,
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"header_field_1\":\"foo\",\"header_field_2\":\"bar\",\"header_field_3\":\"boo\"}"
      combinedRecord.schema().fields().asScala.size shouldBe originalRecord
        .headers()
        .size
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "select some fields from headers for struct" in {

      val originalRecord = sinkRecordWithKeyHeaders()

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map(
          originalRecord.topic() -> Map("header_field_1" -> "my_header_alias")),
        keyFields = Map.empty,
        valueFields = Map.empty,
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      val expected = "{\"my_header_alias\":\"foo\"}"
      combinedRecord.schema().fields().asScala.size shouldBe 1
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should return empty record for struct" in {

      val originalRecord = sinkRecordWithKeyHeaders()
      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map.empty,
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      combinedRecord.schema().fields().asScala.size shouldBe 0
    }

    "should ignore fields for struct" in {
      val originalRecord = getTestRecord

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(originalRecord.topic() -> Map("*" -> "*")),
        ignoreFields = Map(originalRecord.topic() -> Set("int_field")),
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = originalRecord.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"id\":\"sink_test-1-1\",\"long_field\":1,\"string_field\":\"foo\"}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should add fields, key fields and headers for schemaless JSON" in {
      val fields = new util.HashMap[String, Any]()
      fields.put("field1", "value1")
      fields.put("field2", 3)
      fields.put("field3", null) // null so ignored
      fields.put("field4", true)
      fields.put("field5", 1.1)
      fields.put("field6", List(1, 2, 3).asJava)

      val keys = new util.HashMap[String, Any]()
      keys.put("key_field1", "key_value1")
      keys.put("key_field2", 3)
      keys.put("key_field3", null)

      val headers = new ConnectHeaders()
      headers.addBoolean("header_boolean", true)
      headers.addInt("header_int", 1)
      headers.addString("header_string", "header_string_value")
      headers.addFloat("header_float", 1.1f)
      headers.addDecimal("header_decimal", new java.math.BigDecimal("2.1"))

      val record =
        new SinkRecord("t", 0, null, keys, null, fields, 0, null, null, headers)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map("t" -> Map("*" -> "*")),
        keyFields = Map("t" -> Map("key_field1" -> "key_field1_alias")),
        valueFields = Map(
          "t" -> Map("field2" -> "field2_alias",
                     "field3" -> "field3",
                     "field4" -> "field4",
                     "field5" -> "field5",
                     "field6" -> "field6")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"key_field1_alias\":\"key_value1\",\"field5\":1.1,\"field4\":true,\"field3\":null,\"field2_alias\":3,\"field6\":[1,2,3],\"header_float\":1.1,\"header_decimal\":2.1,\"header_string\":\"header_string_value\",\"header_int\":1,\"header_boolean\":true}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should add fields, key fields and headers for JSON with string schema" in {

      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "field3":""
          |}
        """.stripMargin

      val keyJson =
        """
          |{
          |   "key_field1":"value1",
          |   "key_field2":3,
          |   "key_field3":""
          |}
        """.stripMargin

      val record = new SinkRecord("t",
                                  0,
                                  Schema.STRING_SCHEMA,
                                  keyJson,
                                  Schema.STRING_SCHEMA,
                                  json,
                                  0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map("t" -> Map("key_field1" -> "key_field1_alias")),
        valueFields = Map("t" -> Map("field1" -> "field1_alias")),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"key_field1_alias\":\"value1\",\"field1_alias\":\"value1\"}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should return empty value" in {

      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "field3":""
          |}
        """.stripMargin

      val keyJson =
        """
          |{
          |   "key_field1":"value1",
          |   "key_field2":3,
          |   "key_field3":""
          |}
        """.stripMargin

      val record = new SinkRecord("t",
                                  0,
                                  Schema.STRING_SCHEMA,
                                  keyJson,
                                  Schema.STRING_SCHEMA,
                                  json,
                                  0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map.empty,
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should convert key with no headers, no value" in {

      val keyJson =
        """
          |{
          |   "key_field1":"value1",
          |   "key_field2":3,
          |   "key_field3":""
          |}
        """.stripMargin
      val record =
        new SinkRecord("t", 0, Schema.STRING_SCHEMA, keyJson, null, null, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map("t" -> Map("header1" -> "header_alias")),
        keyFields = Map("t" -> Map("key_field1" -> "key_field1_alias")),
        valueFields = Map.empty,
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected = "{\"key_field1_alias\":\"value1\"}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should handle complex type projections in json with schema" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "fieldMap": {
          |     "f1": "v1",
          |     "f2": "v2"
          |   },
          |   "moreComplex": {
          |     "nested1": {
          |       "n1": "nv1",
          |       "arr": [1, 2, 3],
          |       "nested2": {
          |         "n2": "nv2"
          |       }
          |     }
          |   }
          |}
        """.stripMargin

      val record =
        new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(
          "t" -> Map(
            "fieldMap.f1" -> "fm1_alias",
            "moreComplex.nested1.n1" -> "moreComplex.nested1.n1",
            "moreComplex.nested1.arr" -> "moreComplex.nested1.arr",
            "moreComplex.nested1.nested2.n2" -> "moreComplex.nested1.nested2.n2"
          )),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"fm1_alias\":\"v1\",\"moreComplex.nested1.n1\":\"nv1\",\"moreComplex.nested1.arr\":[1,2,3],\"moreComplex.nested1.nested2.n2\":\"nv2\"}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should handle complex type projections in json without schema" in {

      val fields = new util.HashMap[String, Any]()
      fields.put("field1", "value1")
      fields.put("field2", 3)

      val map = new util.HashMap[String, Any]()
      map.put("f1", "v1")
      map.put("f2", "v2")

      fields.put("fieldMap", map)

      val nested = new util.HashMap[String, Any]()
      nested.put("n1", "nv1")
      val arr = new util.ArrayList[Integer]()
      arr.add(1)
      arr.add(2)
      arr.add(3)
      nested.put("arr", arr)

      val moreComplex = new util.HashMap[String, Any]()
      moreComplex.put("nested1", nested)
      fields.put("moreComplex", moreComplex)

      val record =
        new SinkRecord("t", 0, null, null, null, fields, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(
          "t" -> Map(
            "fieldMap.f1" -> "fm1_alias",
            "moreComplex.nested1.n1" -> "moreComplex.nested1.n1",
            "moreComplex.nested1.arr" -> "moreComplex.nested1.arr"
          )),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"fm1_alias\":\"v1\",\"moreComplex.nested1.n1\":\"nv1\",\"moreComplex.nested1.arr\":[1,2,3]}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should handle complex type for struct" in {

      val inner = SchemaBuilder
        .struct()
        .name("inner")
        .version(1)
        .field("long_field", Schema.INT64_SCHEMA)
        .field("string_field", Schema.STRING_SCHEMA)
        .build()

      val innerV = new Struct(inner)
      innerV.put("long_field", 10L)
      innerV.put("string_field", "inner_string")

      val root = SchemaBuilder.struct
        .name("record")
        .version(1)
        .field("id", Schema.STRING_SCHEMA)
        .field("int_field", Schema.INT32_SCHEMA)
        .field("inner", inner)
        .build

      val rootV = new Struct(root)
      rootV.put("id", "my-id")
      rootV.put("int_field", 20)
      rootV.put("inner", innerV)

      val record = new SinkRecord("t", 0, null, null, root, rootV, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(
          "t" -> Map(
            "id" -> "id_alias",
            "inner.long_field" -> "inner_long",
            "inner.string_field" -> "inner.string_field"
          )),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"id_alias\":\"my-id\",\"inner_long\":10,\"inner.string_field\":\"inner_string\"}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }

    "should handle field not found struct" in {

      val inner = SchemaBuilder
        .struct()
        .name("inner")
        .version(1)
        .field("long_field", Schema.INT64_SCHEMA)
        .field("string_field", Schema.STRING_SCHEMA)
        .build()

      val innerV = new Struct(inner)
      innerV.put("long_field", 10L)
      innerV.put("string_field", "inner_string")

      val root = SchemaBuilder.struct
        .name("record")
        .version(1)
        .field("id", Schema.STRING_SCHEMA)
        .field("int_field", Schema.INT32_SCHEMA)
        .field("inner", inner)
        .build

      val rootV = new Struct(root)
      rootV.put("id", "my-id")
      rootV.put("int_field", 20)
      rootV.put("inner", innerV)

      val record = new SinkRecord("t", 0, null, null, root, rootV, 0)

      val projections = new Projections(
        targets = Map.empty,
        headerFields = Map.empty,
        keyFields = Map.empty,
        valueFields = Map(
          "t" -> Map(
            "id" -> "id_alias",
            "inner.long_field" -> "inner_long",
            "inner.string_field" -> "inner.string_field"
          )),
        ignoreFields = Map.empty,
        primaryKeys = Map.empty,
        writeMode = Map.empty,
        ttl = Map.empty,
        batchSize = Map.empty,
        autoCreate = Map.empty,
        autoEvolve = Map.empty,
        partitionBy = Map.empty,
        formats = Map.empty,
        tags = Map.empty,
        withType = Map.empty,
        dynamicTarget = Map.empty,
        keyDelimiters = Map.empty,
        subscriptions = Map.empty,
        sessions = Map.empty,
        keys = Map.empty,
        acks = Map.empty,
        errorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
        errorRetries = 0,
        kcqls = Set.empty
      )

      val combinedRecord = record.newFilteredRecordAsStruct(projections)

      val expected =
        "{\"id_alias\":\"my-id\",\"inner_long\":10,\"inner.string_field\":\"inner_string\"}"
      simpleJsonConverter
        .fromConnectData(combinedRecord.schema(), combinedRecord)
        .toString shouldBe expected
    }
  }
}
