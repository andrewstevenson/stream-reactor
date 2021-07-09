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

import cats.implicits.catsSyntaxEitherId

/**
  * Ensures the keys coming into the sink are all lower cased.
  */
class LowerCaseKeyConfigDefProcessor extends ConfigDefProcessor {
  override def process(input: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    input.map{
      case (k : String, v) if k.toLowerCase.startsWith("connect.s3") => (k.toLowerCase, v)
      case (k, v) => (k, v)
    }.asRight
  }
}
