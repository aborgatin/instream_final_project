package com.github.aborgatin.stream.finalproject.spark.config

import scala.util.matching.Regex

sealed trait ApplicationType {
  val idPattern: Regex
  val desc: String
}

object ApplicationType {
  private val appTypes: List[ApplicationType] = List(
    StructuredStreamingAppType,
    DStreamAppType,
    UnknownAppType
  )

  def apply(ptn: String): ApplicationType = {
    appTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(UnknownAppType)
  }
}

case object StructuredStreamingAppType extends ApplicationType {
  val idPattern = "^(?i)struct|structured|ss$".r
  val desc = "Structured Streaming application"
}

case object DStreamAppType extends ApplicationType {
  val idPattern = "^(?i)dstream|ds$".r
  val desc = "DStream application"
}

case object UnknownAppType extends ApplicationType {
  val idPattern = "".r
  val desc = "unknown"
}

