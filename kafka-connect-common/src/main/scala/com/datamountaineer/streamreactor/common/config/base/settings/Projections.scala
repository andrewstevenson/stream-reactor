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

package com.datamountaineer.streamreactor.common.config.base.settings

import com.datamountaineer.kcql._
import com.datamountaineer.streamreactor.common.errors.ErrorPolicy
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet

case class Projections(targets: Map[String, String], // source -> target
                       writeMode: Map[String, WriteModeEnum],
                       headerFields: Map[String, Map[String, String]],
                       keyFields: Map[String, Map[String, String]],
                       valueFields: Map[String, Map[String, String]],
                       ignoreFields: Map[String, Set[String]],
                       primaryKeys: Map[String, Set[String]],
                       ttl: Map[String, Long],
                       batchSize: Map[String, Int],
                       autoCreate: Map[String, Boolean],
                       autoEvolve: Map[String, Boolean],
                       formats: Map[String, FormatType],
                       partitionBy: Map[String, Set[String]],
                       tags: Map[String, Set[Tag]],
                       withType: Map[String, String],
                       dynamicTarget: Map[String, String],
                       keyDelimiters: Map[String, String],
                       sessions: Map[String, String],
                       subscriptions: Map[String, String],
                       acks: Map[String, Boolean],
                       keys: Map[String, Seq[String]],
                       errorPolicy: ErrorPolicy,
                       errorRetries: Int,
                       kcqls: Set[Kcql])

object Projections extends StrictLogging {
  def apply(kcqls: Set[Kcql],
            errorPolicy: ErrorPolicy,
            errorRetries: Int,
            defaultBatchSize: Int): Projections = {
    new Projections(
      targets = getTargetMapping(kcqls),
      writeMode = getWriteMode(kcqls),
      headerFields = getHeaderFields(kcqls),
      keyFields = getKeyFields(kcqls),
      valueFields = getValueFields(kcqls),
      ignoreFields = kcqls
        .map(rm =>
          (rm.getSource, rm.getIgnoredFields.asScala.map(f => f.getName).toSet))
        .toMap,
      primaryKeys = getPrimaryKeyCols(kcqls),
      ttl = getTTL(kcqls),
      batchSize = getBatchSize(kcqls, defaultBatchSize),
      autoCreate = getAutoCreate(kcqls),
      autoEvolve = getAutoEvolve(kcqls),
      formats = getFormat(kcqls),
      partitionBy = getPartitionByFields(kcqls),
      tags = getTags(kcqls),
      withType = getWithType(kcqls),
      dynamicTarget = getDynamicTarget(kcqls),
      errorPolicy = errorPolicy,
      errorRetries = errorRetries,
      keyDelimiters = getKeyDelimiter(kcqls),
      sessions = getSessions(kcqls),
      subscriptions = getSubscriptions(kcqls),
      acks = getAcks(kcqls),
      keys = getKeys(kcqls),
      kcqls = kcqls
    )
  }

  def getAutoCreate(kcql: Set[Kcql]): Map[String, Boolean] = {
    kcql.map(r => (r.getSource, r.isAutoCreate)).toMap
  }

  def getAutoEvolve(kcql: Set[Kcql]): Map[String, Boolean] = {
    kcql.map(r => (r.getSource, r.isAutoEvolve)).toMap
  }

  def getTTL(kcql: Set[Kcql]): Map[String, Long] = {
    kcql.
      filterNot(r => r.getTTL == 0)
      .map(r => (r.getSource, r.getTTL)).toMap
  }

  def getPrimaryKeyCols(kcql: Set[Kcql]): Map[String, Set[String]] = {
    kcql
      .map(
        k =>
          (k.getSource,
           ListSet(k.getPrimaryKeys.asScala.map(p => p.getName).reverse: _*)))
      .toMap
  }

  def getBatchSize(kcql: Set[Kcql], defaultBatchSize: Int): Map[String, Int] = {
    kcql
      .map(r =>
        (r.getSource, if (r.getBatchSize == 0) defaultBatchSize else r.getBatchSize))
      .toMap
  }

  def getFormat(kcql: Set[Kcql]): Map[String, FormatType] = {
    kcql.toList.map(r => (r.getSource, r.getFormatType)).toMap
  }

  def getUpsertKeys(
      kcqls: Set[Kcql],
      preserveFullKeys: Boolean = false): Map[String, Set[String]] = {

    kcqls
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys: Set[String] = ListSet(
          r.getPrimaryKeys.asScala
            .map(key =>
              if (preserveFullKeys) {
                key.toString
              } else {
                key.getName
            })
            .reverse: _*)
        if (keys.isEmpty)
          throw new ConfigException(
            s"[${r.getTarget}] is set up with upsert, you need to set primary keys")
        (r.getSource, keys)
      }
      .toMap
  }

  def getWriteMode(kcql: Set[Kcql]): Map[String, WriteModeEnum] = {
    kcql.toList.map(r => (r.getSource, r.getWriteMode)).toMap
  }

  def getTargetMapping(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k =>
      k.getSource -> k.getTarget
    }.toMap
  }

  def getValueFields(kcql: Set[Kcql]): Map[String, Map[String, String]] = {
    kcql.map { k =>
      k.getSource -> k.getFields.asScala
        .filterNot(f =>
          f.getName.startsWith("_key.") || f.getName.startsWith("_header."))
        .map(f => {
          val name = if (f.hasParents) {
            s"${f.getParentFields.asScala.mkString(".")}.${f.getName}"
          } else {
            f.getName
          }

          name -> f.getAlias
        })
        .toMap
    }.toMap
  }

  def getKeyFields(kcql: Set[Kcql]): Map[String, Map[String, String]] = {
    kcql.map { k =>
      k.getSource -> k.getFields.asScala
        .filter(f => f.getName.startsWith("_key."))
        .map(f => {
          val name = if (f.hasParents) {
            s"${f.getParentFields.asScala.mkString(".")}.${f.getName}"
          } else {
            f.getName
          }

          name.replaceFirst("_key.", "") -> f.getAlias
        })
        .toMap
    }.toMap
  }

  def getHeaderFields(kcql: Set[Kcql]): Map[String, Map[String, String]] = {
    kcql.map { k =>
      k.getSource -> k.getFields.asScala
        .filter(f => f.getName.startsWith("_header."))
        .map(f => {
          val name = if (f.hasParents) {
            s"${f.getParentFields.asScala.mkString(".")}.${f.getName}"
          } else {
            f.getName
          }

          name.replaceFirst("_header.", "") -> f.getAlias
        })
        .toMap
    }.toMap
  }

  def getPartitionByFields(kcql: Set[Kcql]): Map[String, Set[String]] = {
    kcql
      .filter( k => k.getPartitionBy.hasNext)
      .map { k =>
      k.getSource -> k.getPartitionBy.asScala.toSet
    }.toMap
  }

  def getTags(kcql: Set[Kcql]): Map[String, Set[Tag]] = {
    kcql
      .filterNot(k => k.getTags == null || k.getTags.isEmpty)
      .map { k =>
           k.getSource -> k.getTags.asScala.toSet
      }.toMap
  }

  def getWithType(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k =>
      k.getSource -> k.getWithType
    }.toMap
  }

  def getDynamicTarget(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k =>
      k.getSource -> k.getDynamicTarget
    }.toMap
  }

  def getDocType(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k =>
      k.getSource -> k.getDocType
    }.toMap
  }

  def getBucketing(kcql: Set[Kcql]): Map[String, Bucketing] = {
    kcql.map { k =>
      k.getSource -> k.getBucketing
    }.toMap
  }

  def getKeyDelimiter(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k =>
      k.getSource -> k.getKeyDelimeter
    }.toMap
  }

  def getWithKeys(kcql: Set[Kcql]): Map[String, Set[String]] = {
    kcql.map { k =>
      k.getSource -> k.getWithKeys.asScala.toSet
    }.toMap
  }

  def getSessions(kcql: Set[Kcql]): Map[String, String] = {
    kcql
      .filterNot(k => Option(k.getWithSession).isEmpty)
      .map(k => (k.getSource, k.getWithSession))
      .toMap
  }

  def getSubscriptions(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map(k => (k.getSource, k.getWithSubscription)).toMap
  }

  def getAcks(kcqls: Set[Kcql]): Map[String, Boolean] = {
    kcqls.map(k => (k.getSource, k.getWithAck)).toMap
  }

  def getKeys(kcql: Set[Kcql]): Map[String, Seq[String]] = {
    kcql
      .map(
        k =>
          k.getSource -> Option(k.getWithKeys)
            .map(l => l.asScala)
            .getOrElse(Seq.empty))
      .toMap
  }
}
