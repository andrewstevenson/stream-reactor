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

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LowerCaseKeyConfigDefProcessorTest extends AnyFlatSpec with Matchers {

  val processor = new LowerCaseKeyConfigDefProcessor()

  "lower case key processor" should "leave non-relevant keys alone" in {
    processor.process(Map("ABC" -> "XYZ")) should be (Right(Map("ABC" -> "XYZ")))
  }

  "lower case key processor" should "alter upper case keys" in {
    processor.process(Map("connect.s3.ABC" -> "XYZ")) should be (Right(Map("connect.s3.abc" -> "XYZ")))
  }

  "lower case key processor" should "not change lower case keys" in {
    processor.process(Map("connect.s3.abc" -> "XYZ")) should be (Right(Map("connect.s3.abc" -> "XYZ")))
  }

  "lower case key processor" should "not crash with empty map" in {
    processor.process(Map()) should be (Right(Map()))
  }

  "lower case key processor" should "work with real example keys" in {
    processor.process(Map(
      AWS_ACCESS_KEY.toUpperCase() -> "identity",
      AWS_SECRET_KEY -> "credential",
      AUTH_MODE.toUpperCase() -> "Credentials",
      CUSTOM_ENDPOINT -> "http://127.0.0.1:8099",
      ENABLE_VIRTUAL_HOST_BUCKETS.toUpperCase() -> "true",
    )) should be (Right(Map(
      AWS_ACCESS_KEY -> "identity",
      AWS_SECRET_KEY -> "credential",
      AUTH_MODE -> "Credentials",
      CUSTOM_ENDPOINT -> "http://127.0.0.1:8099",
      ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    )))
  }
}
