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

package com.datamountaineer.streamreactor.connect.cassandra.source

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.{Collections, Date}
import com.datamountaineer.kcql.FormatType
import com.datamountaineer.streamreactor.common.offsets.OffsetHandler
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraSourceSetting, TimestampType}
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraResultSetWrapper.resultSetFutureToScala
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraUtils
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 20/04/16.
  * stream-reactor
  */
class CassandraTableReader(private val name: String,
                           private val session: Session,
                           private val setting: CassandraSourceSetting,
                           private val context: SourceTaskContext,
                           var queue: LinkedBlockingQueue[SourceRecord]) extends StrictLogging {

  private val config = setting.kcql
  private val cqlGenerator = new CqlGenerator(setting)

  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'")
  private val primaryKeyCol = setting.primaryKeyColumn.getOrElse("")
  private val querying = new AtomicBoolean(false)
  private val stop = new AtomicBoolean(false)
  private val table = config.getSource
  private val topic = config.getTarget
  private val keySpace = setting.keySpace
  private val preparedStatementNoOffset = getPreparedStatementNoOffset
  private val preparedStatement = getPreparedStatements
  private var tableOffset: Option[String] = buildOffsetMap(context)
  private val timeSliceDuration: Long = setting.timeSliceDuration
  private val timeSliceDelay: Long = setting.timeSliceDelay
  private var timeSliceValue: Long = timeSliceDuration
  private val sourcePartition = Collections.singletonMap(CassandraConfigConstants.ASSIGNED_TABLES, table)
  private val schemaName = s"$keySpace.$table".replace('-', '.')
  private val bulk = if (setting.timestampColType.equals(TimestampType.NONE)) true else false
  private var schema: Option[Schema] = None
  private val ignoreList = config.getIgnoredFields.asScala.map(_.getName).toSet
  private val isTokenBased = cqlGenerator.isTokenBased()
  private val isDSESearchBased = cqlGenerator.isDSESearchBased()
  private val isBucketBased = cqlGenerator.isBucketBased()
  private val cassandraTypeConverter : CassandraTypeConverter =
    new CassandraTypeConverter(session.getCluster.getConfiguration.getCodecRegistry, setting)
  private var structColDefs: List[ColumnDefinitions.Definition] = _

  /**
    * Build a map of table to offset.
    *
    * @param context SourceTaskContext for this task.
    * @return The last stored offset.
    */
  private def buildOffsetMap(context: SourceTaskContext): Option[String] = {
    val offsetStorageKey = CassandraConfigConstants.ASSIGNED_TABLES
    val tables = List(table)
    val recoveredOffsets = OffsetHandler.recoverOffsets(offsetStorageKey, tables.asJava, context)
    val offset = OffsetHandler.recoverOffset[String](recoveredOffsets, offsetStorageKey, table, primaryKeyCol)
    offset.foreach(s => logger.info(s"Recovered offset $s for connector $name"))
    cqlGenerator.getDefaultOffsetValue(offset)
  }

  /**
    * Build a preparedStatement for the given table.
    *
    * @return the PreparedStatement
    */
  private def getPreparedStatements: PreparedStatement = {
    val selectStatement = cqlGenerator.getCqlStatement
    val statement = session.prepare(selectStatement)
    setting.consistencyLevel.foreach(statement.setConsistencyLevel)
    statement
  }

  private def getPreparedStatementNoOffset: PreparedStatement = {
    val selectStatement = cqlGenerator.getCqlStatementNoOffset
    val statement = session.prepare(selectStatement)
    setting.consistencyLevel.foreach(statement.setConsistencyLevel)
    statement
  }

  /**
    * Fires Cassandra queries and increments the timestamp
    * Every Row returned from query is put into the queue for processing.
    */
  def read(): Unit = if (!stop.get() && !querying.get()) query()

  private def query(): Unit = {
    // we are going to execute the query
    querying.set(true)

    // execute the query, gives us back a future result set
    val frs = if (bulk) {
      // bulk
      resultSetFutureToScala(fireQuery(preparedStatement))
    } else if (isTokenBased) {
      // token based 
      tableOffset match {
        // use offset/token to page thru results
        case Some(tableOffset) => resultSetFutureToScala(bindAndFireTokenQuery(tableOffset))
        // no offset so just read first set of rows
        case None => resultSetFutureToScala(fireQuery(preparedStatementNoOffset))
      }
    } else {
      // time based
      resultSetFutureToScala(bindAndFireTimebasedQuery())
    }

    //give futureresultset to the process method to extract records,
    //once complete it will update the tableoffset
    process(frs)
  }

  /**
    * Bind and execute the preparedStatement and set querying to true.
    *
    * @return a ResultSet.
    */
  private def bindAndFireTimebasedQuery() = {
    // get the lower bound for the timebased query
    // using either the default value or the timestamp of the last 
    // row that was processed (captured in the offset)
    val previousDate = dateFormatter.parse(cqlGenerator.getDefaultOffsetValue(tableOffset).get)
    val previous = previousDate.toInstant
    // we want to manage how close our query is to the "present"
    // so we don't miss data 
    // and we also don't want to have the upper bound
    // be less than the last offset (previous)
    val nowWithDelay = Instant.now().minusMillis(timeSliceDelay)
    val potentialUpperBound = if (nowWithDelay.compareTo(previous) <= 0) previous else nowWithDelay
    
    val upperBound = if (previousDate.getYear == 0) {
      // TODO: we can't do small time slices if default is Jan 1, 1900
      // so for now advance to current date time
      potentialUpperBound
    } else {
      // we want to process in small time slices but never in the future
      // and we also need to have a time slice that grows
      // when no data was found
      val maxTimeSlice = previous.plusMillis(timeSliceValue)
      if (potentialUpperBound.compareTo(maxTimeSlice) <= 0) potentialUpperBound else maxTimeSlice
    }

    // logging the CQL
    val formattedPrevious = previous.toString()
    val formattedNow = upperBound.toString()
    val bound = if (isDSESearchBased) {
      val solrWhere = s"$primaryKeyCol:{$formattedPrevious TO $formattedNow]"
      // we need that to be able to page results even with the solr_query being used, that's why we use the paging and sort configs
      val solrQuery = "{\"q\": \"" + solrWhere + "\", \"sort\":\"" + primaryKeyCol + " asc\", \"paging\":\"driver\"}"
      logger.debug(s"Connector $name query ${preparedStatement.getQueryString} executing with bindings ($solrQuery).")
      preparedStatement.bind(solrQuery)
    } else if (isBucketBased) {
      val buckets = CassandraUtils.getBucketsBetweenDates(previous, upperBound, setting.bucketMode, setting.bucketFormat)

      logger.info(s"Connector $name query ${preparedStatement.getQueryString} executing with bindings ($formattedPrevious, $formattedNow, $buckets).")
      preparedStatement.bind(Date.from(previous), Date.from(upperBound), buckets)
    } else {
      logger.debug(s"Connector $name query ${preparedStatement.getQueryString} executing with bindings ($formattedPrevious, $formattedNow).")
      preparedStatement.bind(Date.from(previous), Date.from(upperBound))
    }
    // bind the offset and db time
    bound.setFetchSize(setting.fetchSize)
    session.executeAsync(bound)
  }

  /**
    * Bind and execute the preparedStatement and set querying to true.
    *
    * @param lastToken The last token from previous query
    * @return a ResultSet.
    */
  private def bindAndFireTokenQuery(lastToken: String) = {
    val bound = preparedStatement.bind(java.util.UUID.fromString(lastToken))
    logger.debug(s"Connector $name query ${preparedStatement.getQueryString} executing with bindings ($lastToken).")
    bound.setFetchSize(setting.fetchSize)
    session.executeAsync(bound)
  }

  /**
    * Execute the preparedStatement and set querying to true.
    *
    * @return a ResultSet.
    */
  private def fireQuery(ps: PreparedStatement): ResultSetFuture = {
    //bind the offset and db time
    val bound = ps.bind()
    //execute the query
    logger.debug(s"Connector $name query ${ps.getQueryString} executing.")
    bound.setFetchSize(setting.fetchSize)
    session.executeAsync(bound)
  }

  /**
    * Iterate over the resultset, extract SourceRecords
    * and add them to the queue.
    *
    * @param future Cassandra Future ResultSet to iterate over.
    */
  private def process(future: Future[ResultSet]): Unit = {
    //get the max offset per query
    var maxOffset: Option[String] = None
    //on success start writing the row to the queue
    future.onComplete {
      case Success(rs) =>
        try {
          logger.debug(s"Connector $name processing results for $keySpace.$table.")
          val iter = rs.iterator()
          var counter = 0

          // process results unless told to stop
          while (iter.hasNext & !stop.get()) {
            // this is asynchronous
            if ((rs.getAvailableWithoutFetching == setting.fetchSize / 2) && !rs.isFullyFetched) rs.fetchMoreResults

            val row = iter.next()
            // if not bulk get the maxOffset value
            if (!bulk) {
              maxOffset = if (isTokenBased) {
                getTokenMaxOffsetForRow(maxOffset, row)
              } else {
                getTimebasedMaxOffsetForRow(maxOffset, row)
              }
              logger.debug(s"Connector $name max Offset is currently: ${maxOffset.get}")
            }
            if (processRow(row)) {
              counter += 1
            }
          }
          logger.debug(s"Connector $name processed $counter row(-s) into $topic topic for table $table")

          alterTimeSliceValueBasedOnRowsProcessess(counter)
          reset(maxOffset)
        }
        catch {
          case t: Throwable =>
            logger.error(s"Connector $name error processing table $keySpace.$table.", t)
            reset(tableOffset)
        }
      case Failure(e) => logger.error("Exception occurred inside future", e)
    }

    //On failure, reset and throw
    future.failed.foreach {
      case t: Throwable =>
        logger.warn(s"Connector $name error querying $table.", t)
        reset(tableOffset)
    }
  }
  
  private def alterTimeSliceValueBasedOnRowsProcessess(rowsProcessed: Int) = {
    // if no rows are processed using the timebased approach
    // we want to increase the range of time we look for and
    // process data so that we don't get stuck
    // this is needed because the current algorithm uses
    // the time stamp/time UUID of the last row processed
    // (see bindAndFireTimebasedQuery)
    timeSliceValue = if (rowsProcessed == 0) {
      // keep adding polling interval to duration when we don't get any hits
      timeSliceValue + timeSliceDuration + setting.pollInterval
    } else {
      // set value back to configured value
      timeSliceDuration
    }
    logger.debug(s"Connector $name time slice value is now $timeSliceValue ms")
  }

  private def getTokenMaxOffsetForRow(maxOffset: Option[String], row: Row): Option[String] = {
    val rowOffsetUuid = extractUuid(row)
    if (rowOffsetUuid.isEmpty) maxOffset else rowOffsetUuid
  }

  private def getTimebasedMaxOffsetForRow(maxOffset: Option[String], row: Row): Option[String] = {
    val rowOffsetDate = extractTimestamp(row)
    if (maxOffset.isEmpty || rowOffsetDate.after(dateFormatter.parse(maxOffset.get))) Some(dateFormatter.format(rowOffsetDate)) else maxOffset
  }

  /**
    * Process a Cassandra row, convert it to a SourceRecord and put in queue
    *
    * @param row The Cassandra row to process.
    *
    */
  private def processRow(row: Row): Boolean = {
    // convert the cassandra row to a struct
    if (structColDefs == null) {
      structColDefs = cassandraTypeConverter.getStructColumns(row, ignoreList)
    }
    val struct = cassandraTypeConverter.convert(row, schemaName, structColDefs, schema)

    // get the offset for this value
    val offset: String = if (isTokenBased) {
      extractUuid(row).getOrElse("")
    } else {
      val rowOffset: Date = if (bulk) dateFormatter.parse(tableOffset.get) else extractTimestamp(row)
      dateFormatter.format(rowOffset)
    }
    logger.debug(s"Connector $name processing row with offset: $offset")

    // create source record
    val record = if (config.isUnwrapping) {
      if (config.getFormatType == FormatType.JSON) {
        val keys = config.getWithKeys
        val v = structColDefs.map(d => if (d.getType == DataType.timestamp) d.getName -> row.getTimestamp(d.getName).getTime else d.getName -> row.getObject(d.getName)).toMap
        val structValue = Json(DefaultFormats).write(v)
        if(keys.isEmpty) {
          new SourceRecord(sourcePartition, Map(primaryKeyCol -> offset).asJava, topic, Schema.STRING_SCHEMA, structValue)
        } else {
          val keyValue = keys.asScala.map(k => row.getObject(k)).mkString(",")
          new SourceRecord(sourcePartition, Map(primaryKeyCol -> offset).asJava, topic, Schema.STRING_SCHEMA, keyValue, Schema.STRING_SCHEMA, structValue)
        }
      } else {
        val structValue = structColDefs.map(d => d.getName).map(name => row.getObject(name)).mkString(",")
        new SourceRecord(sourcePartition, Map(primaryKeyCol -> offset).asJava, topic, Schema.STRING_SCHEMA, structValue)
      }
    } else {
      if (schema.isEmpty) {
        schema = Some(struct.schema())
      }
      new SourceRecord(sourcePartition, Map(primaryKeyCol -> offset).asJava, topic, struct.schema(), struct)
    }

    // add source record to queue
    var added = false
    while (!stop.get() && !added) {
      added = queue.offer(record, 1000, TimeUnit.MILLISECONDS)
    }
    added
  }

  /**
    * Extract the CQL UUID timestamp and return a date
    *
    * @param row The row to extract the timestamp from
    * @return A java.util.Date
    */
  private def extractTimestamp(row: Row): Date = {
    Try(row.getTimestamp(setting.primaryKeyColumn.get)) match {
      case Success(s) => s
      case Failure(_) => new Date(UUIDs.unixTimestamp(row.getUUID(setting.primaryKeyColumn.get)))
    }
  }

  /**
    * extract the CQL primary key (expected to be UUID)
    *
    * @param row The row to extract the UUID
    * @return an Option[String]
    */
  private def extractUuid(row: Row): Option[String] = {
    Try(row.getUUID(setting.primaryKeyColumn.get)) match {
      case Success(s) => Some(s.toString())
      case Failure(_) => None
    }
  }

  /**
    * Set the offset for the table and set querying to false
    *
    * @param offset the date to set the offset to
    */
  private def reset(offset: Option[String]) = {
    //set the offset to the 'now' bind value
    val table = config.getTarget
    logger.debug(s"Connector $name setting offset for $keySpace.$table to $offset.")
    tableOffset = offset.orElse(tableOffset)
    //switch to not querying
    logger.debug(s"Connector $name setting querying for $keySpace.$table to false.")
    querying.set(false)
  }

  /**
    * Closed down the driver session and cluster.
    */
  def close(): Unit = {
    logger.info(s"Connector $name shutting down queries.")
    stopQuerying()
    logger.info(s"Connector $name all stopped.")
  }

  /**
    * Tell me to stop processing.
    */
  def stopQuerying(): Unit = {
    val table = config.getTarget
    stop.set(true)
    while (querying.get()) {
      logger.debug(s"Connector $name waiting for querying to stop for $keySpace.$table.")
      Thread.sleep(2000)
    }
    logger.info(s"Connector $name querying stopped for $keySpace.$table.")
  }

  /**
    * Is the reader in the middle of a query
    */
  def isQuerying: Boolean = {
    querying.get()
  }
}

object CassandraTableReader {
  def apply(name: String,
            session: Session,
            setting: CassandraSourceSetting,
            context: SourceTaskContext,
            queue: LinkedBlockingQueue[SourceRecord]): CassandraTableReader = {
    //return a reader
    new CassandraTableReader(
      name = name,
      session = session,
      setting = setting,
      context = context,
      queue = queue)
  }
}
