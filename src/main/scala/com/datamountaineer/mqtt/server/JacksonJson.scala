package com.datamountaineer.mqtt.server

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._


object JacksonJson {

  //implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val formats = Serialization.formats(NoTypeHints)

  /*def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }*/

  def toJson[T<:AnyRef](value: T): String = {
    write(value)
  }
}