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

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.{
  ERROR_POLICY_PROP_SUFFIX,
  MAX_RETRIES_PROP_SUFFIX
}
import com.datamountaineer.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}

import java.util

object AzureServiceBusConfig {

  val PREFIX = "azure"

  val DEFAULT_BATCH_SIZE = 500

  val AZURE_SAP_KEY = s"$PREFIX.policy.key"
  val AZURE_SAP_NAME = s"$PREFIX.policy.name"

  val KCQL = s"$PREFIX.kcql"

  val AZURE_POLL_INTERVAL = s"poll.interval"
  val AZURE_POLL_INTERVAL_DEFAULT: Long = 0L
  val AZURE_POLL_INTERVAL_DOC =
    "The interval in milliseconds wait before calling Azure Service Bus for records on each poll"

  val AZURE_SB_NAMESPACE = s"$PREFIX.namespace"
  val AZURE_SB_NAMESPACE_DOC = "Fully qualified ServiceBus namespace"
  val AZURE_SB_NAMESPACE_DISPLAY = "Azure ServiceBus namespace"

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
  val SET_HEADERS_DOC =
    """
      |Add session, content type, correlation id, delivery count, sequence number and any application properties on the
      |service bus message to the kafka message headers, source only"
      |""".stripMargin
  val SET_HEADERS_DEFAULT = false

  val config: ConfigDef = new ConfigDef()
    .define(
      AZURE_SAP_NAME,
      Type.STRING,
      null,
      Importance.HIGH,
      "Name of the Shared Access Policy",
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      "Shared Access Policy name"
    )
    .define(
      AZURE_SAP_KEY,
      Type.PASSWORD,
      null,
      Importance.HIGH,
      "Azure key for the Shared Access Policy",
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      "Azure Shared Access Key"
    )
    .define(
      AZURE_SB_NAMESPACE,
      Type.STRING,
      null,
      Importance.HIGH,
      AZURE_SB_NAMESPACE_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      AZURE_SB_NAMESPACE_DISPLAY
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
      SET_HEADERS,
      Type.BOOLEAN,
      SET_HEADERS_DEFAULT,
      Importance.MEDIUM,
      SET_HEADERS_DOC,
      "Configuration",
      1,
      Width.MEDIUM,
      "Set headers"
    )
    .define(
      AZURE_POLL_INTERVAL,
      Type.LONG,
      null,
      Importance.HIGH,
      AZURE_POLL_INTERVAL_DOC,
      "Connection",
      6,
      Width.LONG,
      "Polling frequency"
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

case class AzureServiceBusConfig(props: util.Map[String, String])
    extends BaseConfig(AzureServiceBusConfig.PREFIX,
                       AzureServiceBusConfig.config,
                       props)
    with ErrorPolicySettings
    with KcqlSettings
    with NumberRetriesSettings
