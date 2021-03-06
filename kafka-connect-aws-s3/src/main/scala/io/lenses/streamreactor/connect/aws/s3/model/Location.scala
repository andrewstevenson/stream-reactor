/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model

import com.amazonaws.services.s3.internal.BucketNameUtils
import com.typesafe.scalalogging.LazyLogging

import java.io.File
import java.util.UUID


sealed trait Location {
  def path: String
}

case object RemoteRootLocation {
  def apply(bucketAndPath: String): RemoteRootLocation = {
    bucketAndPath.split(":") match {
      case Array(bucket) => RemoteRootLocation(bucket, None)
      case Array(bucket, path) => RemoteRootLocation(bucket, Some(path))
      case _ => throw new IllegalArgumentException("Invalid number of arguments provided to create BucketAndPrefix")
    }
  }
}

case class RemoteRootLocation(
                            bucket: String,
                            prefix: Option[String]
                          )  extends Location {

  BucketNameUtils.validateBucketName(bucket)

  prefix
    .filter(_.contains("/"))
    .foreach(_ => throw new IllegalArgumentException("Nested prefix not currently supported"))

  override def path: String = s"$bucket/$prefix"
}

case class RemotePathLocation(
                           bucket: String,
                           override val path: String
                         ) extends Location {

  BucketNameUtils.validateBucketName(bucket)

}


object LocalLocation extends LazyLogging {
  def apply(parentDir : LocalLocation, bucketAndPath: RemotePathLocation): LocalLocation = {
    val uuid = UUID.randomUUID().toString

    val dir = new File(s"${parentDir.path}/${bucketAndPath.bucket}/${bucketAndPath.path}")
    logger.info("Creating dir {}", dir)
    dir.mkdirs()

    val file = new File(dir, s"/$uuid")
    logger.info("Creating file {}", file)
    file.createNewFile()
    LocalLocation(file.getAbsolutePath)
  }
}
case class LocalLocation (
                           override val path: String,
                         ) extends Location {



}
