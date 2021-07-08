/*
 *
 *  * Copyright 2017 Datamountaineer.
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

package io.lenses.streamreactor.connect.azure.servicebus.source

import com.datamountaineer.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils

import java.util
import scala.collection.JavaConverters._

class AzureServiceBusSourceConnector extends SourceConnector {

  private var configProps: util.Map[String, String] = _
  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit =
    configProps = props


  override def taskClass(): Class[_ <: Task] =
    classOf[AzureServiceBusSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val raw = configProps.get(AzureServiceBusConfig.KCQL)

    require(raw != null && raw.nonEmpty,
            s"No [${AzureServiceBusConfig.KCQL}] provided!")

    //sql1, sql2
    val kcqls = raw.split(";")
    val groups =
      ConnectorUtils.groupPartitions(kcqls.toList.asJava, maxTasks).asScala

    //split up the kcql statement based on the number of tasks.
    groups
      .filterNot(g => g.asScala.isEmpty)
      .map(g => {
        val taskConfigs = new java.util.HashMap[String, String]
        taskConfigs.putAll(configProps)
        taskConfigs
          .put(AzureServiceBusConfig.KCQL, g.asScala.mkString(";")) //overwrite
        taskConfigs.asScala.asJava
      })
  }.asJava

  override def stop(): Unit = {}

  override def config(): ConfigDef = AzureServiceBusConfig.config

  override def version(): String = manifest.version()
}
