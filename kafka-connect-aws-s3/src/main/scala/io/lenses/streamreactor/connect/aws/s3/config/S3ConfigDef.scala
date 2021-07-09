
/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.config

import cats.implicits.catsSyntaxEitherId
import com.datamountaineer.streamreactor.common.config.base.traits.{BaseConfig, ConnectionSettings, ErrorPolicySettings, KcqlSettings, NumberRetriesSettings, UserSettings}
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.processors.{ConfigDefProcessor, DeprecationConfigDefProcessor, LowerCaseKeyConfigDefProcessor, YamlProfileProcessor}
import io.lenses.streamreactor.connect.aws.s3.model.S3WriteMode.{BuildLocal, Streamed}

import java.util
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

object S3ConfigDef {

  import S3ConfigSettings._

  val config: ConfigDef = new S3ConfigDef()
    .define(
      AWS_ACCESS_KEY,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      "AWS access key"
    )
    .define(
      AWS_SECRET_KEY,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      "AWS password key"
    )
    .define(
      AUTH_MODE,
      Type.STRING,
      AuthMode.Default.toString,
      Importance.HIGH,
      "Authenticate mode, 'env' or 'default'"
    )
    .define(
      CUSTOM_ENDPOINT,
      Type.STRING,
      "",
      Importance.LOW,
      "Custom S3-compatible endpoint (usually for testing)"
    )
    .define(
      ENABLE_VIRTUAL_HOST_BUCKETS,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      "Enable virtual host buckets"
    )
    .define(
      DISABLE_FLUSH_COUNT,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      "Disable flush on reaching count"
    )
    .define(
      WRITE_MODE,
      Type.STRING,
      Streamed.entryName,
      Importance.HIGH,
      s"Write mode, '${Streamed.entryName}' or '${BuildLocal.entryName}'"
    )
    .define(
      LOCAL_TMP_DIRECTORY,
      Type.STRING,
      "",
      Importance.LOW,
      s"Local tmp directory for use with ${BuildLocal.entryName} write mode"
    )
    .define(KCQL_CONFIG, Type.STRING, Importance.HIGH, KCQL_DOC)
    .define(ERROR_POLICY,
      Type.STRING,
      ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      ERROR_POLICY)

    .define(NBR_OF_RETRIES,
      Type.INT,
      NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      NBR_OF_RETRIES)

    .define(ERROR_RETRY_INTERVAL,
      Type.LONG,
      ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      ERROR_RETRY_INTERVAL)

    .define(HTTP_NBR_OF_RETRIES,
      Type.INT,
      HTTP_NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      HTTP_NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      HTTP_NBR_OF_RETRIES)

    .define(HTTP_ERROR_RETRY_INTERVAL,
      Type.LONG,
      HTTP_ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      HTTP_ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      HTTP_ERROR_RETRY_INTERVAL)

}

class S3ConfigDef() extends ConfigDef with LazyLogging {

  private val processorChain: List[ConfigDefProcessor] = List(new LowerCaseKeyConfigDefProcessor, new DeprecationConfigDefProcessor, new YamlProfileProcessor)

  override def parse(jProps: util.Map[_, _]): util.Map[String, AnyRef] = {
    val scalaProps: Map[Any, Any] = jProps.asScala.toMap
    processProperties(scalaProps) match {
      case Left(exception) => throw exception
      case Right(value) => super.parse(value.asJava)
    }
  }

  private def processProperties(scalaProps: Map[Any, Any]): Either[Throwable, Map[Any, Any]] = {
    val stringProps = scalaProps.collect { case (k: String, v: AnyRef) => (k.toLowerCase, v) }
    val nonStringProps = scalaProps -- stringProps.keySet
    processStringKeyedProperties(stringProps) match {
      case Left(exception) => exception.asLeft[Map[Any,Any]]
      case Right(stringKeyedProps) => (nonStringProps ++ stringKeyedProps).asRight
    }
  }

  def writeInOrder(remappedProps: Map[String, Any]) : ListMap[String,Any] = ListMap(remappedProps.toSeq.sortBy(_._1):_*)

  def processStringKeyedProperties(stringProps: Map[String,Any]): Either[Throwable, Map[String, Any]] = {
    var remappedProps: Map[String, Any] = stringProps
    for (proc <- processorChain) {
      logger.info("START: Executing ConfigDef processor {} with props {}", proc.getClass.getSimpleName, writeInOrder(remappedProps))
      proc.process(remappedProps) match {
        case Left(exception) => return exception.asLeft[Map[String,AnyRef]]
        case Right(properties) => remappedProps = properties
      }
      logger.info("END: Executing ConfigDef processor {} with props {}", proc.getClass.getSimpleName, writeInOrder(remappedProps))
    }
    remappedProps.asRight
  }

}

case class S3ConfigDefBuilder(sinkName: Option[String], props: util.Map[String, String])
  extends BaseConfig(S3ConfigSettings.CONNECTOR_PREFIX, S3ConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings
    with S3FlushSettings
    with S3WriteModeSettings {

  def getParsedValues : Map[String,_] = values().asScala.toMap

}


