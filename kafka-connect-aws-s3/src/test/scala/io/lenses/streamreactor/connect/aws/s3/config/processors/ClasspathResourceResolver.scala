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

import com.typesafe.scalalogging.LazyLogging

import java.io.File
import scala.util.Try

object ClasspathResourceResolver extends LazyLogging {

  def getResourcesDirectory(): Either[Throwable, String] = {
    val url = classOf[YamlProfileProcessorTest].getResource("/profiles/")
    Try {
      val uri = url.toURI
      logger.info("Profile uri: {}", uri)
      val profilePath = new File(uri).getAbsolutePath
      logger.info("Profile path: {}", profilePath)
      profilePath
    }.toEither
  }

}
