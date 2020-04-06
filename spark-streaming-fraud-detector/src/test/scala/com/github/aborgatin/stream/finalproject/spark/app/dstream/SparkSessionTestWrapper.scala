package com.github.aborgatin.stream.finalproject.spark.app.dstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }
  lazy val streamingContext: StreamingContext =
    new StreamingContext(spark.sparkContext, Seconds(1))
}
