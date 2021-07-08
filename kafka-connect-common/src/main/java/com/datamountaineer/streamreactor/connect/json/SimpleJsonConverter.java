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

package com.datamountaineer.streamreactor.connect.json;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Implementation of Converter that uses JSON to store schemas and objects.
 *
 * Parts taken from Apache Kafka
 */
public class SimpleJsonConverter {

  public static final SimpleDateFormat ISO_DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  public static final SimpleDateFormat TIME_FORMAT= new SimpleDateFormat("HH:mm:ss.SSSZ");
  static{
    ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  // Convert values in Kafka Connect form into their logical types. These logical converters are discovered by logical type
  // names specified in the field
  private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();

  static {
    TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof byte[]))
          throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
        return Decimal.toLogical(schema, (byte[]) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer))
          throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
        return Date.toLogical(schema, (int) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer))
          throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
        return Time.toLogical(schema, (int) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Long))
          throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
        return Timestamp.toLogical(schema, (long) value);
      }
    });
  }

  public JsonNode fromConnectData(Schema schema, Object value) {
    return convertToJson(schema, value);
  }

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
   * and the converted object.
   */
  private static JsonNode convertToJson(Schema schema, Object logicalValue) {
    if (logicalValue == null) {
      if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
        return null;
      if (schema.defaultValue() != null)
        return convertToJson(schema, schema.defaultValue());
      if (schema.isOptional())
        return JsonNodeFactory.instance.nullNode();
      throw new DataException("Conversion error: null value for field that is required and has no default value");
    }

    Object value = logicalValue;
    try {
      final Schema.Type schemaType;
      if (schema == null) {
        schemaType = ConnectSchema.schemaType(value.getClass());
        if (schemaType == null)
          throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
      } else {
        schemaType = schema.type();
      }
      switch (schemaType) {
        case INT8:
          return JsonNodeFactory.instance.numberNode((Byte) value);
        case INT16:
          return JsonNodeFactory.instance.numberNode((Short) value);
        case INT32:
          if (schema != null && Date.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(ISO_DATE_FORMAT.format((java.util.Date) value));
          }
          if (schema != null && Time.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(TIME_FORMAT.format((java.util.Date) value));
          }
          return JsonNodeFactory.instance.numberNode((Integer) value);
        case INT64:
          String schemaName = schema.name();
          if(Timestamp.LOGICAL_NAME.equals(schemaName)){
            return JsonNodeFactory.instance.numberNode(Timestamp.fromLogical(schema, (java.util.Date) value));
          }
          return JsonNodeFactory.instance.numberNode((Long) value);
        case FLOAT32:
          return JsonNodeFactory.instance.numberNode((Float) value);
        case FLOAT64:
          return JsonNodeFactory.instance.numberNode((Double) value);
        case BOOLEAN:
          return JsonNodeFactory.instance.booleanNode((Boolean) value);
        case STRING:
          CharSequence charSeq = (CharSequence) value;
          return JsonNodeFactory.instance.textNode(charSeq.toString());
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.numberNode((BigDecimal) value);
          }

          byte[] valueArr = null;
          if (value instanceof byte[])
            valueArr = (byte[]) value;
          else if (value instanceof ByteBuffer)
            valueArr = ((ByteBuffer) value).array();

          if (valueArr == null)
            throw new DataException("Invalid type for bytes type: " + value.getClass());

          return JsonNodeFactory.instance.binaryNode(valueArr);

        case ARRAY: {
          Collection collection = (Collection) value;
          ArrayNode list = JsonNodeFactory.instance.arrayNode();
          for (Object elem : collection) {
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode fieldValue = convertToJson(valueSchema, elem);
            list.add(fieldValue);
          }
          return list;
        }
        case MAP: {
          Map<?, ?> map = (Map<?, ?>) value;
          // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
          boolean objectMode;
          if (schema == null) {
            objectMode = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              if (!(entry.getKey() instanceof String)) {
                objectMode = false;
                break;
              }
            }
          } else {
            objectMode = schema.keySchema().type() == Schema.Type.STRING;
          }
          ObjectNode obj = null;
          ArrayNode list = null;
          if (objectMode)
            obj = JsonNodeFactory.instance.objectNode();
          else
            list = JsonNodeFactory.instance.arrayNode();
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode mapKey = convertToJson(keySchema, entry.getKey());
            JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

            if (objectMode)
              obj.set(mapKey.asText(), mapValue);
            else
              list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
          }
          return objectMode ? obj : list;
        }
        case STRUCT: {
          Struct struct = (Struct) value;
          if (struct.schema() != schema)
            throw new DataException("Mismatching schema.");
          ObjectNode obj = JsonNodeFactory.instance.objectNode();
          for (Field field : schema.fields()) {
            obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
          }
          return obj;
        }
      }

      throw new DataException("Couldn't convert " + value + " to JSON.");
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }

  private interface JsonToConnectTypeConverter {
    Object convert(Schema schema, JsonNode value);
  }

  private interface LogicalTypeConverter {
    Object convert(Schema schema, Object value);
  }
}
