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

package io.lenses.streamreactor.connect.azure

import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfig
import org.apache.kafka.common.config.ConfigException

import scala.util.{Failure, Success, Try}

package object servicebus extends StrictLogging {

  object TargetType extends Enumeration {
    type TargetType = Value
    val QUEUE, TOPIC = Value
  }

  def getConverters(converters: Map[String, String], conf: Map[String, String]): Map[String, Converter] = {
    converters.map { case (topic, clazz) =>
      logger.info(s"Creating converter instance for $clazz")
      val converter = Try(Class.forName(clazz).newInstance()) match {
        case Success(value) => value.asInstanceOf[Converter]
        case Failure(_) => throw new ConfigException(s"Invalid [${AzureServiceBusConfig.KCQL}] is invalid. [$clazz] should have an empty ctor!")
      }
      converter.initialize(conf)
      topic -> converter
    }
  }
}
