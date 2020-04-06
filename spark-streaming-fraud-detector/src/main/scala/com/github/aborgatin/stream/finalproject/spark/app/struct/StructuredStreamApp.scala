package com.github.aborgatin.stream.finalproject.spark.app.struct

import java.util.concurrent.TimeUnit

import com.github.aborgatin.stream.finalproject.spark.app.StreamApp
import com.github.aborgatin.stream.finalproject.spark.config.{Config, UserClickSchema}
import com.github.aborgatin.stream.finalproject.spark.datasource.DataConnector
import com.github.aborgatin.stream.finalproject.spark.model.{Bot, UserClick, UserClickBot}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

case class StructuredStreamApp(private val spark: SparkSession,
                               private val config: Config,
                               private val cacheDataConnector: DataConnector[Bot],
                               private val storageDataConnector: DataConnector[UserClickBot]
                              ) extends StreamApp {

  private val watermark = s"${config.eventsWatermark} minutes"

  override def processWithAwait(): Unit = {
    val streamSystemParam = config.streamSystemParam
    val df = spark
      .readStream
      .format(streamSystemParam.format)
      .options(streamSystemParam.getConfig)
      .load()

    import spark.implicits._

    val monitor = new Object
    val uuid = udf(() => java.util.UUID.randomUUID.toString)

    def notify(): Unit = {
      warn("bot monitor notify" )
      monitor.synchronized {
        monitor.notifyAll()
      }
      warn("bot after monitor notify" )
    }
    def wait(): Unit = {
      warn("event monitor wait")
      monitor.synchronized {
        monitor.wait()
      }
      warn("event after monitor wait")
    }

    warn(s"Watermark - $watermark, threshold - ${config.getBotRequests()} requests per ${config.botSeconds} with slide ${config.botSlideSeconds}")


    //Prepare all incoming event to process
    val ds = df.selectExpr("CAST(value AS STRING) as json")
      .select(from_json('json, UserClickSchema.schema) as 'data)
      .na.drop()
      .select($"data.payload.ip", $"data.payload.type", $"data.payload.event_time", $"data.payload.url")
      .withColumnRenamed("type", "event_type")
      //      .withColumn("event_time", ($"event_time" / 1000).cast("timestamp"))
      .as[UserClick]
      .where($"event_type" === "click")
      .withColumn("event_id", uuid())


    val dsBot = ds
      .withWatermark("event_time", watermark)
      .groupBy(window($"event_time", config.botSeconds, config.botSlideSeconds),
        $"ip").agg(
      count("event_id").as("events_count"),
      max($"event_time").as("last_event_time")
    )
      .where($"events_count" >= config.getBotRequests())
      .drop("events_count", "window")

    dsBot
      .as[Bot]
      .writeStream
      .trigger(Trigger.ProcessingTime(config.getLatency(), TimeUnit.SECONDS))
      .outputMode(OutputMode.Update())
      .foreachBatch((ds, l) => {
        cacheDataConnector.write(ds)
        notify()
      })
      .start()

    ds
      .join(cacheDataConnector.getDataFrame(), Seq("ip"), "leftOuter")
      .withColumn("is_bot", $"last_event_time".isNotNull)
      .drop("last_event_time")
      .as[UserClickBot]
      .writeStream
      .trigger(Trigger.ProcessingTime(config.getLatency(), TimeUnit.SECONDS))
      .outputMode(OutputMode.Append())
      .foreachBatch((ds, l) => {
        wait()
        storageDataConnector.write(ds)
      })
      .start()

    spark.streams.awaitAnyTermination()
  }
}
