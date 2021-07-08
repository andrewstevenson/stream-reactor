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

package io.lenses.streamreactor.connect.azure.storage.config

import com.datamountaineer.kcql.FormatType
import com.datamountaineer.streamreactor.common.config.base.settings.Projections
import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.TargetType
import io.lenses.streamreactor.connect.azure.storage.config.AbstractConfigExtensions._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import scala.util.{Failure, Success, Try}

case class AzureStorageSettings(
    account: String,
    accountKey: Password,
    endpoint: Option[String],
    projections: Projections,
    targetType: Map[String, TargetType.Value],
    datePartitionFormat: String,
    converters: Map[String, String],
    encode: Map[String, Boolean],
    lock: Map[String, Int],
    setHeaders: Boolean
)

object AzureStorageSettings extends StrictLogging {
  def apply(config: AzureStorageConfig): AzureStorageSettings = {

    val account =
      config.getStringOrThrowOnNull(AzureStorageConfig.AZURE_ACCOUNT)
    val accountKey =
      config.getPasswordOrThrowOnNull(AzureStorageConfig.AZURE_ACCOUNT_KEY)
    val endpoint =
      Option(config.getString(AzureStorageConfig.AZURE_ENDPOINT))
    val kcqls = config.getKCQL
    val defaultPartitionFormat =
      config.getString(AzureStorageConfig.PARTITION_DATE_FORMAT)
    val errorPolicy = config.getErrorPolicy
    val maxRetries = config.getNumberRetries
    val targetType =
      kcqls.toList
        .map(k => {
          val storeAs = Option(k.getStoredAs) match {
            case Some(value) => TargetType.withName(value.toUpperCase)
            case None =>
              TargetType.TABLE
          }
          k.getSource -> storeAs
        })
        .toMap


    val converters = kcqls
      .map(k => {
        (k.getSource,
         if (k.getWithConverter == null)
           classOf[BytesConverter].getCanonicalName
         else k.getWithConverter)
      })
      .toMap

    converters.values.foreach(clazz =>
      Try(Class.forName(clazz)) match {
        case Failure(_) =>
          throw new ConfigException(
            s"Invalid [${AzureStorageConfig.KCQL}]. [$clazz] not found")
        case Success(clz) =>
          if (!classOf[Converter].isAssignableFrom(clz)) {
            throw new ConfigException(
              s"Invalid [${AzureStorageConfig.KCQL}]. [$clazz] is not inheriting Converter")
          }
    })

    val encode = kcqls.map(k => (k.getTarget, k.getWithEncodeBase64)).toMap

    val lock = kcqls
      .map(
        k =>
          (k.getSource,
           if (k.getWithLockTime > 0) k.getWithLockTime.toInt
           else AzureStorageConfig.DEFAULT_LOCK))
      .toMap

    val setHeaders = config.getBoolean(AzureStorageConfig.SET_HEADERS)
    val projections = Projections(kcqls = kcqls, errorPolicy = errorPolicy, errorRetries = maxRetries, defaultBatchSize = 100)

    AzureStorageSettings(
      account = account,
      accountKey = accountKey,
      endpoint = endpoint,
      targetType =  targetType,
      datePartitionFormat = defaultPartitionFormat,
      converters = converters,
      projections = projections,
      encode = encode,
      lock = lock,
      setHeaders = setHeaders
    )
  }

  private def getFormatType(format: FormatType): FormatType = {
    if (format == null) {
      FormatType.JSON
    } else {
      format match {
        case FormatType.JSON | FormatType.BINARY =>
        case _                                   => throw new ConnectException(s"Unknown WITHFORMAT type")
      }
      format
    }
  }
}
