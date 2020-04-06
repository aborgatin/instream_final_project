package com.github.aborgatin.stream.finalproject.spark.config

import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

object UserClickSchema {
  val schema: StructType = new StructType()
    .add("payload", new StructType()
      .add("ip", StringType)
      .add("type", StringType)
      .add("event_time", TimestampType)
      .add("url", StringType)
    )

}
