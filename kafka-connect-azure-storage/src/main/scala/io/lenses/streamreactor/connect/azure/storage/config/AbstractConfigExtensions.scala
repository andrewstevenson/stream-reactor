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

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

object AbstractConfigExtensions {
  implicit class AbstractConfigExtension(val config: AbstractConfig)
    extends AnyVal {
    def getStringOrThrowOnNull(field: String): String =
      Option(config.getString(field)).getOrElse(raiseException(field))

    def getPasswordOrThrowOnNull(field: String): Password = {
      val password = config.getPassword(field)
      if (password == null) raiseException(field)
      password
    }

    private def raiseException(fieldName: String) = throw new ConnectException(
      s"$fieldName not set"
    )
  }
}