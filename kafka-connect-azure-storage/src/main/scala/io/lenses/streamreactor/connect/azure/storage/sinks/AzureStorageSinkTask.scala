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

import com.datamountaineer.streamreactor.common.errors.{
  ErrorPolicy,
  ErrorPolicyEnum,
  RetryErrorPolicy
}
import com.datamountaineer.streamreactor.common.utils.{
  JarManifest,
  ProgressCounter
}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.{
  AzureStorageConfig,
  AzureStorageSettings
}
import io.lenses.streamreactor.connect.azure.storage.sinks.writers.AzureStorageWriter
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import java.util
import scala.collection.JavaConverters._

class AzureStorageSinkTask extends SinkTask with StrictLogging {

  var writer: Option[AzureStorageWriter] = None
  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation)
  private var enableProgress: Boolean = false
  private val progressCounter = new ProgressCounter

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      scala.io.Source
        .fromInputStream(getClass.getResourceAsStream("/storage-ascii.txt"))
        .mkString + s" ${version()}")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()

    AzureStorageConfig.config.parse(conf)
    val configBase = new AzureStorageConfig(conf)
    enableProgress =
      configBase.getBoolean(AzureStorageConfig.PROGRESS_COUNTER_ENABLED)
    val settings = AzureStorageSettings(configBase)

    //if error policy is retry set retry interval
    settings.projections.errorPolicy match {
      case RetryErrorPolicy() =>
        context.timeout(
          configBase.getInt(AzureStorageConfig.ERROR_RETRY_INTERVAL).toLong)

      case _ =>
    }

    writer = Some(AzureStorageWriter(settings))
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    writer.foreach(w => w.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()
}
