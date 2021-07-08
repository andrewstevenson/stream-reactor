/*
 *
 *  * Copyright 2020 lensesio
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

package io.lenses.streamreactor.connect.azure

import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueClient}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageConfig
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import scala.util.{Failure, Success, Try}

package object storage extends StrictLogging {

  object TargetType extends Enumeration {
    type TargetType = Value
    val QUEUE, TABLE = Value
  }

  def getConnectionString(account: String,
                          accountKey: Password,
                          endpoint: Option[String],
                          endpointType: String): String = {
    val base =
      s"DefaultEndpointsProtocol=https;AccountName=$account;AccountKey=${accountKey.value()};"

    endpoint match {
      case Some(e) => s"$base$endpointType=$e;"
      case None    => s"${base}EndpointSuffix=core.windows.net"
    }
  }

  def getStorageAccount(account: String,
                        accountKey: Password,
                        endpoint: Option[String],
                        endpointType: String): CloudStorageAccount = {
    Try(CloudStorageAccount.parse(
      getConnectionString(account, accountKey, endpoint, endpointType))) match {
      case Success(cloudStorageAccount) => cloudStorageAccount
      case Failure(exception) =>
        throw new ConnectException(
          s"Failed to connection to Azure storage account [$account] at endpoint [$endpoint], type [$endpointType]",
          exception)
    }
  }

  def getConverters(converters: Map[String, String], conf: Map[String, String]): Map[String, Converter] = {
    converters.map { case (topic, clazz) =>
      logger.info(s"Creating converter instance for $clazz")
      val converter = Try(Class.forName(clazz).newInstance()) match {
        case Success(value) => value.asInstanceOf[Converter]
        case Failure(_) => throw new ConfigException(s"Invalid [${AzureStorageConfig.KCQL}] is invalid. [$clazz] should have an empty ctor!")
      }
      converter.initialize(conf)
      topic -> converter
    }
  }

  def getQueueClient(account: String, accountKey: Password, endpoint: Option[String]): CloudQueueClient = {
    val storageAccount = getStorageAccount(account,
      accountKey,
      endpoint,
      "")

    Try(storageAccount.createCloudQueueClient()) match {
      case Success(client) => client
      case Failure(exception) =>
        throw new ConnectException("Failed to create cloud table client",
          exception)
    }
  }

  def getQueueReferences(client: CloudQueueClient, queueNames: Map[String, (Boolean, Boolean)]): Map[String, CloudQueue] = {
    queueNames.map{ case (queueName, (autocreate, encode)) =>
      val queueRef = client.getQueueReference(queueName)

      if (encode) queueRef.setShouldEncodeMessage(true)

      if (queueRef.exists()) {
        queueName -> queueRef
      } else {
        if (autocreate) {
          Try(queueRef.createIfNotExists()) match {
            case Success(_) =>
              logger.info(s"Queue [$queueName] created")
              queueName -> queueRef

            case Failure(e) => throw new ConnectException(s"Failed to create queue [$queueName]", e)
          }
        } else {
          throw new ConfigException(s"Queue [$queueName] not found")
        }
      }
    }
  }
}
