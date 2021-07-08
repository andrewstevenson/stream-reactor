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

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst._
import com.datamountaineer.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}

import java.util

object AzureStorageConfig {

  val PREFIX = "azure"

  val QUEUE_SOURCE_MAX_BATCH_SIZE = 32
  val TABLE_DEFAULT_BATCH_SIZE = 100
  val DEFAULT_LOCK = 30
  val HEADER_MESSAGE_ID = "messageId"
  val HEADER_REMOVED = "removed"
  val HEADER_DEQUEUE_COUNT = "dequeueCount"

  val AZURE_ACCOUNT = s"$PREFIX.account"
  val AZURE_ACCOUNT_DOC = "Azure storage account to write to."
  val AZURE_ACCOUNT_KEY = s"$PREFIX.account.key"
  val AZURE_ACCOUNT_KEY_DOC = "Azure storage account key."
  val AZURE_ENDPOINT = s"$PREFIX.storage.endpoint"
  val AZURE_ENDPOINT_DOC = "Azure storage endpoint for the storage account (CosmosDB Table Storage only)."
  val PARTITION_DATE_FORMAT = s"$PREFIX.partition.date.format"
  val PARTITION_DATE_FORMAT_DEFAULT = "YYYY-MM-dd"
  val PARTITION_DATE_FORMAT_DOC =
    "Partition date format for default record timestamp partitioning (Table Storage only)"

  val KCQL = s"$PREFIX.$KCQL_PROP_SUFFIX"

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC =
    "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val ERROR_RETRY_INTERVAL = s"$PREFIX.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val ERROR_POLICY = s"$PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val NBR_OF_RETRIES = s"$PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val SET_HEADERS = s"$PREFIX.set_headers"
  val SET_HEADERS_DOC = "Add message id and dequeue count to message headers, source only"
  val SET_HEADERS_DEFAULT = false


  val config: ConfigDef = new ConfigDef()
    .define(
      AZURE_ACCOUNT,
      Type.STRING,
      null,
      Importance.HIGH,
      AZURE_ACCOUNT_DOC,
      "Connection",
      1,
      Width.LONG,
      "Storage account"
    )
    .define(
      AZURE_ACCOUNT_KEY,
      Type.PASSWORD,
      null,
      Importance.HIGH,
      AZURE_ACCOUNT_KEY_DOC,
      "Connection",
      2,
      Width.LONG,
      "Storage account key"
    )
    .define(
      AZURE_ENDPOINT,
      Type.STRING,
      null,
      Importance.HIGH,
      AZURE_ENDPOINT_DOC,
      "Connection",
      3,
      Width.LONG,
      "Storage endpoint"
    )
    .define(
      KCQL,
      Type.STRING,
      null,
      Importance.HIGH,
      "KCQL statement",
      "KCQL",
      1,
      Width.LONG,
      "KCQL"
    )
    .define(
      PARTITION_DATE_FORMAT,
      Type.STRING,
      PARTITION_DATE_FORMAT_DEFAULT,
      Importance.MEDIUM,
      PARTITION_DATE_FORMAT_DOC,
      "Configuration",
      2,
      Width.MEDIUM,
      "Table storage partition data format"
    )
    .define(
      SET_HEADERS,
      Type.BOOLEAN,
      SET_HEADERS_DEFAULT,
      Importance.MEDIUM,
      SET_HEADERS_DOC,
      "Configuration",
      3,
      Width.MEDIUM,
      "Set headers"
    )
    .define(
      PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      PROGRESS_COUNTER_ENABLED_DISPLAY
    )
    .define(ERROR_POLICY,
            Type.STRING,
            ERROR_POLICY_DEFAULT,
            Importance.HIGH,
            ERROR_POLICY_DOC,
            "Errors",
            1,
            ConfigDef.Width.MEDIUM,
            ERROR_POLICY)
    .define(
      ERROR_RETRY_INTERVAL,
      Type.INT,
      ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      ERROR_RETRY_INTERVAL_DOC,
      "Errors",
      2,
      ConfigDef.Width.MEDIUM,
      ERROR_RETRY_INTERVAL
    )
    .define(NBR_OF_RETRIES,
            Type.INT,
            NBR_OF_RETIRES_DEFAULT,
            Importance.MEDIUM,
            NBR_OF_RETRIES_DOC,
            "Errors",
            3,
            ConfigDef.Width.MEDIUM,
            NBR_OF_RETRIES)
}

case class AzureStorageConfig(props: util.Map[String, String])
    extends BaseConfig(AzureStorageConfig.PREFIX,
                       AzureStorageConfig.config,
                       props)
    with ErrorPolicySettings
    with KcqlSettings
    with NumberRetriesSettings
