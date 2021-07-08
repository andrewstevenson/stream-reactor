/*
 *
 *  * Copyright 2020 Lenses.io.
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

package io.lenses.streamreactor.connect.azure.storage.sinks.writers

import com.datamountaineer.kcql.WriteModeEnum
import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.common.rowkeys.{StringGenericRowKeyBuilder, StringStructFieldsStringKeyBuilder}
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.microsoft.azure.storage.table.{CloudTableClient, DynamicTableEntity, EntityProperty, TableBatchOperation}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.{AzureStorageConfig, AzureStorageSettings}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object AzureTableStorageWriter {
  def apply(settings: AzureStorageSettings,
            tableClient: CloudTableClient): AzureTableStorageWriter =
    new AzureTableStorageWriter(settings, tableClient)
}

class AzureTableStorageWriter(settings: AzureStorageSettings,
                              client: CloudTableClient)
    extends Writer
    with StrictLogging
    with ErrorHandler {

  //initialize error tracker
  initialize(settings.projections.errorRetries, settings.projections.errorPolicy)

  private val cloudTableMap = settings.projections.targets.map {
    case (_, table) =>
      val tableRef = client.getTableReference(table)

      if (tableRef.exists()) {
        table -> tableRef
      } else {
        if (settings.projections.autoCreate(table)) {

          if (!table.matches("^[a-zA-Z0-9]*$")) {
            throw new ConnectException(
              s"Invalidate table name [$table]. Only alpha numeric characters allowed by Azure")
          }

          Try(tableRef.createIfNotExists()) match {
            case Success(_) =>
              logger.info(s"Table [$table] created")
              table -> tableRef

            case Failure(e) =>
              throw new ConnectException(s"Failed to create table [$table]", e)
          }

        } else {
          throw new ConfigException(s"Table [$table] not found")
        }
      }
  }

  val dateFormatter = new SimpleDateFormat(settings.datePartitionFormat)

  // write the records to a table
  override def write(table: String,
                     records: Seq[SinkRecord],
                     batchSize: Int): Unit = {
    records
      .grouped(if (batchSize > 0) batchSize else AzureStorageConfig.TABLE_DEFAULT_BATCH_SIZE)
      .foreach(g => writeOperations(table, createBatch(g)))
  }

  // execute the table operations
  def writeOperations(table: String,
                      operations: Seq[TableBatchOperation]): Unit = {
    operations
      .foreach(operation => {
        handleTry(Try(cloudTableMap(table).execute(operation)))
      })
    logger.debug(s"Sent [${operations.size}] records to table [$table]")
  }

  // create the table operations
  def createBatch(records: Seq[SinkRecord]): Seq[TableBatchOperation] = {

    records.map { record =>
      val batchOperation = new TableBatchOperation()
      settings.projections.writeMode(record.topic()) match {
        case WriteModeEnum.INSERT =>
          batchOperation.insert(processRecords(record))
        case WriteModeEnum.UPSERT =>
          batchOperation.insertOrMerge(processRecords(record))
        case WriteModeEnum.UPDATE =>
          batchOperation.insertOrReplace(processRecords(record))
      }

      batchOperation
    }
  }

  // construct the entities to insert
  def processRecords(record: SinkRecord): DynamicTableEntity = {

    val dynamic = new DynamicTableEntity()

    //set the row key, must be unique
    dynamic.setRowKey(new StringGenericRowKeyBuilder("-").build(record))

    Option(record.value()) match {
      case Some(_) =>
        // select the required fields
//        val converted =
//          convert(record,
//                  settings
//                    .fieldsMap(record.topic())
//                    .map(f => f.getName -> f.getAlias)
//                    .toMap)
//            .value()
//            .asInstanceOf[Struct]



        // set the payload
        // build the table entities from the record
        val entities = getEntityProperties(record.newFilteredRecordAsStruct(settings.projections))
        dynamic.setProperties(entities)

        // set partition by fields set in the KCQL statement if PARTITIONBY set
        if (settings.projections.partitionBy
              .getOrElse(record.topic(), Set.empty)
              .nonEmpty) {
          dynamic.setPartitionKey(
            StringStructFieldsStringKeyBuilder(
              settings.projections.partitionBy(record.topic()).toSeq,
              settings.projections.keyDelimiters.getOrElse(record.topic(), "-")).build(record))
        } else {
          // set the partition to be the record timestamp, formatted by default to YYY-MM-DD
          dynamic.setPartitionKey(
            dateFormatter.format(new util.Date(record.timestamp())))
        }

      case None =>
        logger.warn(
          s"Empty payload received on topic [${record.topic()}]. No payload set. Message discarded")
    }

    dynamic
  }

  // look up the fields in the message and add a edm types to the properties
  def getEntityProperties(
      record: Struct): util.HashMap[String, EntityProperty] = {
    val hash = new util.HashMap[String, EntityProperty]()

    record
      .schema()
      .fields()
      .asScala
      .foreach(f => {

        val value = Option(record.get(f.name())) match {
          case Some(value) => Some(value)
          case None =>
            logger.debug(s"Discarding empty field [${f.name()}]")
            None
        }

        // rename reserved fields
        val name = f.name().toLowerCase match {
          case "id"           => "_id"
          case "timestamp"    => "_timestamp"
          case "rowkey"       => "_rowkey"
          case "partitionkey" => "_partitionkey"
          case n @ _          => n
        }

        value.foreach(v => {
          entityFromLogicalType(v, f)
            .orElse(entityFromFieldType(v, f))
            .foreach(e => hash.put(name, e))
        })

      })
    hash
  }

  // convert from logical types to edm for Azure
  def entityFromLogicalType(value: AnyRef,
                            field: Field): Option[EntityProperty] = {
    Option(field.schema().name()).collect {
      case Date.LOGICAL_NAME =>
        value.asInstanceOf[Any] match {
          case d: java.util.Date =>
            new EntityProperty(d)

          case i: Int =>
            new EntityProperty(Date.toLogical(field.schema, i))

          case _ =>
            throw new IllegalArgumentException(
              s"Can't convert [$value] to Date for schema [${field.schema().`type`()}]")
        }
      case Time.LOGICAL_NAME =>
        value.asInstanceOf[Any] match {
          case _: Int =>
            new EntityProperty(
              Time.toLogical(field.schema, value.asInstanceOf[Int]))

          case d: java.util.Date =>
            new EntityProperty(d)

          case _ =>
            throw new IllegalArgumentException(
              s"Can't convert [$value] to Date for schema [${field.schema().`type`()}]")
        }
      case Timestamp.LOGICAL_NAME =>
        value.asInstanceOf[Any] match {
          case l: Long =>
            new EntityProperty(Timestamp.toLogical(field.schema, l))

          case d: java.util.Date =>
            new EntityProperty(d)

          case _ =>
            throw new IllegalArgumentException(
              s"Can't convert [$value] to Date for schema [${field.schema().`type`()}]")
        }
    }
  }

  // convert from struct to edm type
  def entityFromFieldType(value: AnyRef,
                          field: Field): Option[EntityProperty] = {

    field.schema.`type`() match {
      case Type.STRING =>
        Some(new EntityProperty(value.toString))

      case Type.INT8 =>
        Some(new EntityProperty(value.asInstanceOf[Byte]))

      case Type.INT16 | Type.INT32 =>
        Some(new EntityProperty(value.asInstanceOf[Int]))

      case Type.INT64 =>
        Some(new EntityProperty(value.asInstanceOf[Long]))

      case Type.BOOLEAN =>
        Some(new EntityProperty(value.asInstanceOf[Boolean]))

      case Type.BYTES =>
        Some(new EntityProperty(value.asInstanceOf[Array[Byte]]))

      case Type.FLOAT32 =>
        Some(new EntityProperty(value.toString.toFloat))

      case Type.FLOAT64 =>
        Some(new EntityProperty(value.asInstanceOf[Double]))

      case _ =>
        logger.warn(
          s"Unsupported type [${field.schema().toString}] for field [${field.name}]. Discarding field.")
        None
    }
  }
}
