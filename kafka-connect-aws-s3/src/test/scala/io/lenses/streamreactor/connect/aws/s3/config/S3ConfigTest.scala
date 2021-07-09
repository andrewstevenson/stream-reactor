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

import com.datamountaineer.streamreactor.common.errors.{NoopErrorPolicy, RetryErrorPolicy, ThrowErrorPolicy}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.types.Password
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class S3ConfigTest extends AnyFlatSpec with Matchers with LazyLogging {

  "S3Config" should "get string value" in {
    val map = Map(
      "defined" -> "hello",
      "blank" -> ""
    )
    S3Config.getString(map, "defined") should be (Some("hello"))
    S3Config.getString(map, "blank") should be (None)
    S3Config.getString(map, "undefined") should be (None)
  }

  "S3Config" should "get password value" in {
    val map = Map(
      "definedPass" -> new Password("hello"),
      "emptyPass" -> new Password(""),
      "definedString" -> "defined",
      "emptyString" -> "",
    )
    S3Config.getPassword(map, "definedPass") should be (Some("hello"))
    S3Config.getPassword(map, "emptyPass") should be (None)
    S3Config.getPassword(map, "undefined") should be (None)
    S3Config.getPassword(map, "definedString") should be (Some("defined"))
    S3Config.getPassword(map, "emptyString") should be (None)

  }

  "S3Config" should "get boolean value" in {
    val map = Map(
      "trueBoolProp" -> true,
      "falseBoolProp" -> false,
      "trueStringProp" -> "true",
      "falseStringProp" -> "false",
      "emptyProp" -> ""
    )
    S3Config.getBoolean(map, "trueBoolProp") should be (Some(true))
    S3Config.getBoolean(map, "falseBoolProp") should be (Some(false))
    S3Config.getBoolean(map, "trueStringProp") should be (Some(true))
    S3Config.getBoolean(map, "falseStringProp") should be (Some(false))
    S3Config.getBoolean(map, "emptyProp") should be (None)
    S3Config.getBoolean(map, "undefinedProp") should be (None)
  }

  "S3Config" should "set error policies in a case insensitive way" in {

    val errorPolicyValuesMap = Table(
      ("testName", "value", "errorPolicyClass"),
      ("lcvalue-noop", "noop", NoopErrorPolicy()),
      ("lcvalue-throw", "throw", ThrowErrorPolicy()),
      ("lcvalue-retry", "retry", RetryErrorPolicy()),
      ("ucvalue-noop", "NOOP", NoopErrorPolicy()),
      ("ucvalue-throw", "THROW", ThrowErrorPolicy()),
      ("ucvalue-retry", "RETRY", RetryErrorPolicy()),
      ("value-unspecified", "", ThrowErrorPolicy()),
    )

    forAll(errorPolicyValuesMap) {
     (name, value, clazz) =>
       logger.debug("Executing {}", name)
       S3Config( Map("connect.s3.error.policy" -> value)).errorPolicy should be(clazz)
    }
  }

  val retryValuesMap = Table(
    ("testName", "retries", "interval", "result"),
    ("noret-noint",  0, 0, RetryConfig(0, 0)),
    ("ret-and-int",  1, 2, RetryConfig(1, 2)),
    ("noret-noint-strings",  "0", "0", RetryConfig(0, 0)),
    ("ret-and-int-strings",  "1", "2", RetryConfig(1, 2)),
  )

  "S3Config" should "set retry config" in {
    forAll(retryValuesMap) {
      (name: String, ret: Any, interval: Any, result: RetryConfig) =>
        logger.debug("Executing {}", name)
        S3Config( Map(
          "connect.s3.max.retries" -> ret,
          "connect.s3.retry.interval" -> interval)
        ).connectorRetryConfig should be(result)
    }
  }

  "S3Config" should "set http retry config" in {
    forAll(retryValuesMap) {
      (name: String, ret: Any, interval: Any, result: RetryConfig) =>
        logger.debug("Executing {}", name)
        S3Config( Map(
          "connect.s3.http.max.retries" -> ret,
          "connect.s3.http.retry.interval" -> interval)
        ).httpRetryConfig should be(result)
    }
  }
}
