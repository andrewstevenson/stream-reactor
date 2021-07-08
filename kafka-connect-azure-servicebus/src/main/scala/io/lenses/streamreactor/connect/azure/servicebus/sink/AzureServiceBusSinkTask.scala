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

package io.lenses.streamreactor.connect.azure.servicebus.sink

import com.datamountaineer.streamreactor.common.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.servicebus.config.{AzureServiceBusConfig, AzureServiceBusSettings}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import java.util
import scala.collection.JavaConverters._

class AzureServiceBusSinkTask extends SinkTask with StrictLogging {

  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation)
  private var enableProgress: Boolean = false
  private val progressCounter = new ProgressCounter
  var writer: Option[AzureServiceBusWriter] = None

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      scala.io.Source
        .fromInputStream(getClass.getResourceAsStream("/servicebus-ascii.txt"))
        .mkString + s" ${version()}")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()
    AzureServiceBusConfig.config.parse(conf)
    val configBase = new AzureServiceBusConfig(conf)
    enableProgress =
      configBase.getBoolean(AzureServiceBusConfig.PROGRESS_COUNTER_ENABLED)

    val serviceBusSettings = AzureServiceBusSettings(configBase)
    writer = Some(AzureServiceBusWriter(serviceBusSettings))
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    writer.foreach(_.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  override def stop(): Unit = writer.foreach(_.close())
  override def version(): String = manifest.version()
}
