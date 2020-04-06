package com.github.aborgatin.stream.finalproject.spark.util

import java.sql.Timestamp

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

case class UnixTimestampDeserializer() extends JsonDeserializer[Timestamp] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Timestamp = {
    new Timestamp(p.getLongValue * 1000)
  }
}