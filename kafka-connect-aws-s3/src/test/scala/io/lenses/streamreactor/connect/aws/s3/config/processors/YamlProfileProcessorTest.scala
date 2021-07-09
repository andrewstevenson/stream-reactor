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
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.KCQL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YamlProfileProcessorTest extends AnyFlatSpec with Matchers with LazyLogging {

  "process" should "load in single example yaml file" in {
    val result = process(Map(), "/example.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(12)
    r should contain allOf(
      "connect.s3.retry.interval" -> 10000,
      "connect.s3.error.policy" -> "RETRY",
      "connect.s3.http.max.retries" -> 20,
      "connect.s3.kcql" -> "INSERT INTO `target-bucket:target-path` SELECT * FROM `source.bucket` STOREAS `text` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000",
      "connect.s3.aws.access.key" -> "myAccessKey",
      "connect.s3.max.retries" -> 10,
      "connect.s3.vhost.bucket" -> true,
      "connect.s3.aws.secret.key" -> "mySecretKey",
      "connect.s3.aws.region" -> "us-east-1",
      "connect.s3.http.retry.interval" -> 20000,
      "connect.s3.custom.endpoint" -> "aws-endpoint.com",
      "connect.s3.aws.auth.mode" -> "Credentials",
    )

  }

  "process" should "load in single Yaml file" in {
    val result = process(Map(), "/default.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(4)
    r should contain allOf(
      "connect.s3.kcql" -> "INSERT INTO `target-bucket:target-path` SELECT * FROM `source.bucket` PARTITIONBY name,title,salary STOREAS `text` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000",
      "connect.s3.aws.access.key" -> "myAccessKey",
      "connect.s3.aws.secret.key" -> "mySecretKey",
      "connect.s3.aws.auth.mode" -> "myAuthMode",
    )
  }

  "process" should "resolve multiple Yaml files" in {
    val result = process(Map(), "/default.yaml", "/override.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(5)
    r should contain allOf(
      "connect.s3.kcql" -> "INSERT INTO `target-bucket:target-path` SELECT * FROM `source.bucket` PARTITIONBY name,title,salary STOREAS `text` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000",
      "connect.s3.aws.access.key" -> "overrideAccessKey",
      "connect.s3.aws.secret.key" -> "mySecretKey",
      "connect.s3.aws.auth.mode" -> "myAuthMode",
      "connect.s3.local.tmp.directory" -> "/my/local/tmp/dir",
    )
  }

  "process" should "merge kcql options" in {
    val result = process(Map(), "/kcql_all.yaml", "/kcql_override.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(1)
    r("connect.s3.kcql") should be("INSERT INTO `myOverrideBucket:myOverridePartition` SELECT * FROM `my-kafka-override-topic` PARTITIONBY name,title,salary STOREAS `parquet` WITHPARTITIONER = Values WITH_FLUSH_SIZE = 1 WITH_FLUSH_INTERVAL = 2 WITH_FLUSH_COUNT = 3")
  }

  "process" should "merge kcql options from kcql string and builder" in {
    val result = process(Map(), "/default.yaml", "/kcql_override.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(4)
    r("connect.s3.kcql") should be("INSERT INTO `myOverrideBucket:myOverridePartition` SELECT * FROM `my-kafka-override-topic` PARTITIONBY name,title,salary STOREAS `parquet` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000")
  }

  "process" should "merge allow overriding kcql properties with connector config" in {
    val result = process(Map(KCQL_CONFIG -> "INSERT INTO expectedTarget SELECT * FROM expectedSource PARTITIONBY name,title,salary STOREAS `parquet` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000"), "/default.yaml", "/kcql_override.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(4)
    r("connect.s3.kcql") should be("INSERT INTO `expectedTarget` SELECT * FROM `expectedSource` PARTITIONBY name,title,salary STOREAS `parquet` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000")
  }

  "process" should "merge allow retaining kcql properties when overriding" in {
    val result = process(Map(KCQL_CONFIG -> "INSERT INTO use_profile SELECT * FROM use_profile PARTITIONBY name,title,salary STOREAS `parquet` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000"), "/default.yaml", "/kcql_override.yaml")

    result should be('Right)
    val r = result.right.get
    r.size should be(4)
    r("connect.s3.kcql") should be("INSERT INTO `myOverrideBucket:myOverridePartition` SELECT * FROM `my-kafka-override-topic` PARTITIONBY name,title,salary STOREAS `parquet` WITH_FLUSH_SIZE = 500000000 WITH_FLUSH_INTERVAL = 3600 WITH_FLUSH_COUNT = 50000")
  }

  private def process(properties: Map[String, String], yamls: String*): Either[Throwable, Map[String, Any]] = {
    ClasspathResourceResolver.getResourcesDirectory() match {
      case Left(ex) => ex.asLeft
      case Right(resourcesDir) => new YamlProfileProcessor().process(Map[String, String](
        "connect.s3.config.profiles" -> yamls.map(resourcesDir + _).mkString(","),
      ).combine(properties))
    }

  }
}
