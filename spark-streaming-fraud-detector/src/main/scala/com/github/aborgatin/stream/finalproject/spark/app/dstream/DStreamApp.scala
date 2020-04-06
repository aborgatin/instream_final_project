package com.github.aborgatin.stream.finalproject.spark.app.dstream

import com.github.aborgatin.stream.finalproject.spark.app.StreamApp
import com.github.aborgatin.stream.finalproject.spark.app.dstream.window.TimeWindow
import com.github.aborgatin.stream.finalproject.spark.config.Config
import com.github.aborgatin.stream.finalproject.spark.datasource.DataConnector
import com.github.aborgatin.stream.finalproject.spark.model.{Bot, UserClickBot, UserClickPayload}
import com.github.aborgatin.stream.finalproject.spark.util.JsonUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

case class DStreamApp(private val spark: SparkSession,
                      private val config: Config,
                      private val cacheWriter: DataConnector[Bot],
                      private val persistentWriter: DataConnector[UserClickBot]
                     ) extends StreamApp {
  override def processWithAwait(): Unit = {
    val latency = config.getLatency() / 2
    val consoleWriter = config.consoleWriter
    val ssc = new StreamingContext(spark.sparkContext, Seconds(latency))
    val streamParam = config.streamSystemParam

    val conf = streamParam.getConfig

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getOrElse("kafka.bootstrap.servers", "localhost:9092"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> conf.getOrElse("kafka.group.id", "gridu_stream_app"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = conf.getOrElse("subscribe", "final")
    debug(s"Topic ${topic}")
    debug(s"Params - $kafkaParams")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )


    var offsetRanges = Array[OffsetRange]()
    val stream = kafkaStream
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .filter(x => !x.value().isEmpty)
      .map(x => JsonUtil.fromJson[UserClickPayload](x.value()).payload)
      .map(x => UserClickBot(x.ip, x.event_type, x.event_time, x.url, is_bot = false, java.util.UUID.randomUUID.toString))

    val helper = HelperClasses(config)
    val botDStream = helper.findBots(stream)

    botDStream
      .foreachRDD(rdd => {
        import spark.implicits._
        val ds = rdd.toDS()
        cacheWriter.write(ds)
      })

    stream
      .map(click => (click.ip, click))
      .leftOuterJoin(botDStream.map(bot => bot.ip -> bot.last_event_time))
      .map(helper.mapUpdateUserClickAfterJoin)
      .transform(rdd => {
        Thread.sleep(TimeWindow.getIntervalInSeconds(config.botSeconds) * 1000)
        import spark.implicits._
        val cacheRdd = cacheWriter.getDataFrame().as[Bot].rdd.map(bot => (bot.ip, bot.last_event_time))
        rdd.leftOuterJoin(cacheRdd)
      })
      .map(helper.mapUpdateUserClickAfterJoin)
      .map(_._2)
      .foreachRDD(rdd => {
        import spark.implicits._
        val ds = rdd.toDS()
        persistentWriter.write(ds)
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })

    ssc.checkpoint(config.sparkParam.getCheckpointDir)
    ssc.start()
    ssc.awaitTermination()
  }


}

