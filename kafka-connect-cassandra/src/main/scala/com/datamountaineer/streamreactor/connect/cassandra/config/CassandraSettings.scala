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

package com.datamountaineer.streamreactor.connect.cassandra.config

import com.datamountaineer.kcql.{Field, Kcql}
import com.datamountaineer.streamreactor.common.errors.{ErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.cassandra.config.BucketMode.BucketMode
import com.datamountaineer.streamreactor.connect.cassandra.config.DefaultValueServeStrategy.DefaultValueServeStrategy
import com.datamountaineer.streamreactor.connect.cassandra.config.TimestampType.TimestampType
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

/**
  * Created by andrew@datamountaineer.com on 22/04/16. 
  * stream-reactor
  */

trait CassandraSetting

object TimestampType extends Enumeration {
  type TimestampType = Value
  val TIMESTAMP, DSESEARCHTIMESTAMP, TIMEUUID, TOKEN, BUCKETTIMESERIES, NONE = Value
}

object BucketMode extends Enumeration {
  type BucketMode = Value
  val DAY, HOUR, MINUTE, SECOND, NONE = Value
}

object LoadBalancingPolicy extends Enumeration {
  type LoadBalancingPolicy = Value
  val TOKEN_AWARE, ROUND_ROBIN, DC_AWARE_ROUND_ROBIN, LATENCY_AWARE = Value
}

case class CassandraSourceSetting(kcql: Kcql,
                                  keySpace: String,
                                  primaryKeyColumn: Option[String] = None,
                                  timestampColType: TimestampType,
                                  pollInterval: Long = CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
                                  consistencyLevel: Option[ConsistencyLevel],
                                  errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                                  taskRetires: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
                                  fetchSize: Int = CassandraConfigConstants.FETCH_SIZE_DEFAULT,
                                  timeSliceDuration: Long = CassandraConfigConstants.TIMESLICE_DURATION_DEFAULT,
                                  timeSliceDelay: Long = CassandraConfigConstants.TIMESLICE_DELAY_DEFAULT,
                                  initialOffset: String = CassandraConfigConstants.INITIAL_OFFSET_DEFAULT,
                                  timeSliceMillis: Long = CassandraConfigConstants.TIME_SLICE_MILLIS_DEFAULT,
                                  mappingCollectionToJson: Boolean = CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON_DEFAULT,
                                  bucketMode: BucketMode,
                                  bucketFormat: String,
                                  bucketFieldName: String
                                 ) extends CassandraSetting

case class CassandraSinkSetting(keySpace: String,
                                kcqls: Seq[Kcql],
                                fields: Map[String, Seq[Field]],
                                ignoreField: Map[String, Seq[Field]],
                                errorPolicy: ErrorPolicy,
                                threadPoolSize: Int,
                                consistencyLevel: Option[ConsistencyLevel],
                                taskRetries: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
                                enableProgress: Boolean = CassandraConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
                                deleteEnabled: Boolean = CassandraConfigConstants.DELETE_ROW_ENABLED_DEFAULT,
                                deleteStatement: String = CassandraConfigConstants.DELETE_ROW_STATEMENT_DEFAULT,
                                deleteStructFields: Seq[String] = Seq.empty,
                                defaultValueStrategy: Option[DefaultValueServeStrategy] = None
                               ) extends CassandraSetting

/**
  * Cassandra Setting used for both Readers and writers
  * Holds the table, topic, import mode and timestamp columns
  * Import mode and timestamp columns are only applicable for the source.
  **/
object CassandraSettings extends StrictLogging {

  def configureSource(config: CassandraConfigSource): Seq[CassandraSourceSetting] = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(keySpace.nonEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val pollInterval = config.getLong(CassandraConfigConstants.POLL_INTERVAL)

    val bucketFormat = config.getString(CassandraConfigConstants.BUCKET_TIME_SERIES_FORMAT)
    val bucketFieldName = config.getString(CassandraConfigConstants.BUCKET_TIME_SERIES_FIELD_NAME)

    val consistencyLevel = config.getConsistencyLevel
    val errorPolicy = config.getErrorPolicy
    val kcqls = config.getKCQL
    val primaryKeyCols = config.getPrimaryKeyCols()
    val fetchSize = config.getInt(CassandraConfigConstants.FETCH_SIZE)
    val incrementalModes = config.getIncrementalMode(kcqls)
    val timeSliceDuration = config.getLong(CassandraConfigConstants.TIMESLICE_DURATION)
    val timeSliceDelay = config.getLong(CassandraConfigConstants.TIMESLICE_DELAY)
    val initialOffset = config.getString(CassandraConfigConstants.INITIAL_OFFSET)
    val timeSliceMillis = config.getLong(CassandraConfigConstants.TIME_SLICE_MILLIS)
    val mappingCollectionToJson = config.getBoolean(CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON)

    kcqls.map { r =>
      val tCols = primaryKeyCols(r.getSource)
      val timestampType = Try(TimestampType.withName(incrementalModes(r.getSource).toUpperCase)) match {
        case Success(s) => s
        case _ => TimestampType.NONE
      }

      val bucketMode = Try(BucketMode.withName(config.getString(CassandraConfigConstants.BUCKET_TIME_SERIES_MODE))) match {
        case Success(s) => s
        case _ => BucketMode.NONE
      }

      if (timestampType == TimestampType.BUCKETTIMESERIES && bucketMode == BucketMode.NONE) {
        throw new ConfigException("You should specify a bucketMode while using BUCKETTIMESERIES")
      }

      if (bucketMode != BucketMode.NONE && bucketFormat.isEmpty) {
        throw new ConfigException("You should specify a bucketFormat while using BUCKETTIMESERIES")
      }

      if (timestampType != TimestampType.NONE && tCols.size != 1) {
        throw new ConfigException("Only one primary key column is allowed to be specified in Incremental mode. " +
          s"Received ${tCols.mkString(",")} for source ${r.getSource}")
      }

      CassandraSourceSetting(
        kcql = r,
        keySpace = keySpace,
        primaryKeyColumn = tCols.headOption,
        timestampColType = timestampType,
        pollInterval = pollInterval,
        errorPolicy = errorPolicy,
        consistencyLevel = consistencyLevel,
        fetchSize = fetchSize,
        timeSliceDuration = timeSliceDuration,
        timeSliceDelay = timeSliceDelay,
        initialOffset = initialOffset,
        timeSliceMillis = timeSliceMillis,
        mappingCollectionToJson = mappingCollectionToJson,
        bucketMode = bucketMode,
        bucketFormat = bucketFormat,
        bucketFieldName = bucketFieldName
      )
    }.toSeq
  }

  def configureSink(config: CassandraConfigSink): CassandraSinkSetting = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(keySpace.nonEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val errorPolicy = config.getErrorPolicy
    val retries = config.getNumberRetries
    val kcqls = config.getKCQL.toSeq
    val fields = config.getFields()
    val ignoreFields = config.getIgnoreFields()
    val threadPoolSize = config.getThreadPoolSize
    val consistencyLevel = config.getConsistencyLevel

    val enableCounter = config.getBoolean(CassandraConfigConstants.PROGRESS_COUNTER_ENABLED)

    // support for deletion
    val deleteEnabled = config.getBoolean(CassandraConfigConstants.DELETE_ROW_ENABLED)
    val deleteStmt = config.getString(CassandraConfigConstants.DELETE_ROW_STATEMENT)
    // validate
    if (deleteEnabled) require(deleteStmt.nonEmpty, CassandraConfigConstants.DELETE_ROW_STATEMENT_MISSING)

    val structFlds = config.getList(CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS)

    val defaultValueStrategy  = DefaultValueServeStrategy.of(config.getString(CassandraConfigConstants.DEFAULT_VALUE_SERVE_STRATEGY_PROPERTY))

    CassandraSinkSetting(keySpace,
      kcqls,
      fields,
      ignoreFields,
      errorPolicy,
      threadPoolSize,
      consistencyLevel,
      retries,
      enableCounter,
      deleteEnabled,
      deleteStmt,
      structFlds.asScala,
      defaultValueStrategy
    )
  }
}


object DefaultValueServeStrategy extends Enumeration {
  type DefaultValueServeStrategy = Value
  val NULL, UNSET = Value

  def of(value: String): Option[DefaultValueServeStrategy] = {
    Try(withName(value)).toOption
  }
}