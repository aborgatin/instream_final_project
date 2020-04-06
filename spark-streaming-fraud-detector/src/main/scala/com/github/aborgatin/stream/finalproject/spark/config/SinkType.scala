package com.github.aborgatin.stream.finalproject.spark.config

import scala.util.matching.Regex

sealed trait SinkType {
  val idPattern: Regex
  val desc: String
}

object SinkType {
  private val sinkTypes: List[SinkType] = List(
    CassandraSinkType,
    AmazonS3SinkType,
    IgniteSinkType,
    RedisSinkType,
    UnknownSinkType
  )

  def apply(ptn: String): SinkType = {
    sinkTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(UnknownSinkType)
  }
}

case object CassandraSinkType extends SinkType {
  val idPattern = "^(?i)cassandra$".r
  val desc = "Cassandra"
}

case object AmazonS3SinkType extends SinkType {
  val idPattern = "^(?i)amazon|s3|amazon s3$".r
  val desc = "Amazon S3"
}
case object IgniteSinkType extends SinkType {
  val idPattern = "^(?i)ignite|apache ignite$".r
  val desc = "Apache Ignite"
}
case object RedisSinkType extends SinkType {
  val idPattern = "^(?i)redis$".r
  val desc = "Redis"
}

case object UnknownSinkType extends SinkType {
  val idPattern = "".r
  val desc = "unknown"
}
