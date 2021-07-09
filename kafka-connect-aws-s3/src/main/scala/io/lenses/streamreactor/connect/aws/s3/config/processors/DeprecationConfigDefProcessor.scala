/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.config.processors

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.{DEP_AUTH_MODE, DEP_AWS_ACCESS_KEY, DEP_AWS_SECRET_KEY, DEP_CUSTOM_ENDPOINT, DEP_ENABLE_VIRTUAL_HOST_BUCKETS, _}

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
  * Some properties are officially deprecated in the connector.  To allow backwards compatibility, this remaps the old properties to their new string values.
  */
class DeprecationConfigDefProcessor extends ConfigDefProcessor with LazyLogging {

  private val deprecatedProps: Map[String, String] = ListMap(
    DEP_AUTH_MODE -> AUTH_MODE,
    DEP_AWS_ACCESS_KEY -> AWS_ACCESS_KEY,
    DEP_AWS_SECRET_KEY -> AWS_SECRET_KEY,
    DEP_ENABLE_VIRTUAL_HOST_BUCKETS -> ENABLE_VIRTUAL_HOST_BUCKETS,
    DEP_CUSTOM_ENDPOINT -> CUSTOM_ENDPOINT
  )

  override def process(input: Map[String, Any]): Either[Exception, Map[String, Any]] = {

    val mutableInput = mutable.Map(input.toSeq: _*)
    deprecatedProps.foreach {
      case (oldPropKey, newPropKey) =>
        if (mutableInput.contains(oldPropKey)) {
          mutableInput.put(newPropKey, mutableInput(oldPropKey))
          mutableInput.remove(oldPropKey)
        }
    }

    mutableInput.toMap.asRight
  }
}
