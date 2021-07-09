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

package io.lenses.streamreactor.connect.aws.s3.config.processors.kcql

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings

import java.util

/**
  * Given a list of profiles and configurations, the KcqlProcessor pulls out the relevant properties for
  * the Kcql string, and the exploded Kcql configuration (available in Yaml) and merges them until
  * finally reaching a single Kcql string with the combined configurations.
  */
object KcqlProcessor extends LazyLogging {

  def process(configs: List[Map[String, Any]]): Either[Throwable, String] = {

    val (error, validCfgs) = configs.map {
      cfg =>
        for {
          kcqlConf <- getKcqlConfig(cfg)
          builderConf <- getBuilderConfig(cfg)
          cfgs <- singleCfg(Seq(kcqlConf, builderConf).filter(_.nonEmpty))
        } yield cfgs
    }.partition(_.isLeft)

    error.headOption match {
      case Some(Left(value)) => value.asLeft[String]
      case None =>
        KcqlMapToStringConverter
          .convert(
            flattenMap(extractPropMap(validCfgs))
          )
          .left.map(new IllegalStateException(_))
      case Some(Right(_)) => new IllegalStateException("Impossible for this to be Right. Match error avoidance in action.").asLeft
    }
  }

  private def flattenMap(propMap: Seq[Map[KcqlProp, String]]): Map[KcqlProp, String] = propMap.reduce(_ ++ _)

  private def extractPropMap(allCfg: Seq[Either[Throwable, Map[KcqlProp, String]]]): Seq[Map[KcqlProp, String]] = {
    allCfg
      .collect {
        case Right(value: Map[KcqlProp, String]) => value
      }
      .filter(_.nonEmpty)
  }

  private def getBuilderConfig(config: Map[String, Any]): Either[Throwable, Map[KcqlProp, String]] = {
    config.get(S3ConfigSettings.KCQL_BUILDER) match {
      case Some(value: util.Map[_, _]) =>
        YamlToKcqlMapConverter.convert(value)
      case None =>
        Map.empty[KcqlProp, String].asRight
      case other =>
        new IllegalStateException(s"unexpected type specified: $other").asLeft
    }
  }


  private def getKcqlConfig(config: Map[String, Any]): Either[Throwable, Map[KcqlProp, String]] = {
    config.get(S3ConfigSettings.KCQL_CONFIG) match {
      case Some(value: String) => StringToKcqlMapConverter.convert(value)
      case _ => Map.empty[KcqlProp, String].asRight
    }
  }

  private def singleCfg(cfgs: Seq[Map[KcqlProp, String]]): Either[Throwable, Map[KcqlProp, String]] = {
    cfgs.size match {
      case 0 => Map.empty[KcqlProp, String].asRight
      case 1 => cfgs.head.asRight
      case _ => new IllegalArgumentException("Too many configs in one profile").asLeft
    }
  }
}
