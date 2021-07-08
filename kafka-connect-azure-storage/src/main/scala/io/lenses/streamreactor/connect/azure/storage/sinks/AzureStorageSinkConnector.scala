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

package io.lenses.streamreactor.connect.azure.storage.sinks

import com.datamountaineer.streamreactor.common.config.Helpers
import com.datamountaineer.streamreactor.common.utils.JarManifest
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.collection.JavaConverters._

class AzureStorageSinkConnector extends SinkConnector with StrictLogging {
  private var configProps: Option[util.Map[String, String]] = None
  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting [${this.getClass.getCanonicalName}].")
    Helpers.checkInputTopics(AzureStorageConfig.KCQL, props.asScala.toMap)
    configProps = Some(props)
  }

  override def taskClass(): Class[_ <: Task] =
    classOf[AzureStorageSinkTask]

  override def taskConfigs(
      maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps.get).toList.asJava
  }

  override def stop(): Unit = {}

  override def config(): ConfigDef = AzureStorageConfig.config

  override def version(): String = manifest.version()
}
