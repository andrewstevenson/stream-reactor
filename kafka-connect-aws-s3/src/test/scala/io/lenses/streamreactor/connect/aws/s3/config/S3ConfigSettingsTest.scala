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

package io.lenses.streamreactor.connect.aws.s3.config

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
  * Reads the constants in the S3ConfigSettings class and ensures that they are
  * all lower cased for consistency.  This will help protect for regressions in
  * future when adding new properties to this file.  Uses reflection.
  */
class S3ConfigSettingsTest extends AnyFlatSpec with Matchers with LazyLogging {

  private val currentMirror = scala.reflect.runtime.currentMirror
  private val instanceMirror = currentMirror.reflect(S3ConfigSettings)
  private val ignorePropertiesWithSuffix = Set("_DOC", "_DEFAULT", "_DISPLAY")

  "S3ConfigSettings" should "ensure all keys are lower case" in {

    val docs = getMembersForClass(S3ConfigSettings)
      .collect {
        case m: MethodSymbol if m.isGetter && m.isPublic => (m.fullName, getValue(m))
      }
      .filterNot{
        case (k, _) => ignorePropertiesWithSuffix.exists(k.contains(_))
      }

    docs should have size(23)
    docs.foreach {
      case (k, v) => {
        logger.info("method: {}, value: {}", k, v)
        v.toLowerCase should be(v)
      }
    }
  }

  private def getMembersForClass(clazz: S3ConfigSettings.type) = {
    currentMirror
      .classSymbol(clazz.getClass)
      .toType
      .members
  }

  private def getValue(method: universe.MethodSymbol) = {
    instanceMirror.reflectMethod(method).apply().toString()
  }

}
