

package com.datamountaineer.streamreactor.connect.mongodb

import org.json4s._
import org.json4s.native.JsonMethods._

object Json {
  implicit val formats = DefaultFormats

  def fromJson[T <: Product:Manifest](json: String): T = {
    parse(json).extract[T]
  }
}