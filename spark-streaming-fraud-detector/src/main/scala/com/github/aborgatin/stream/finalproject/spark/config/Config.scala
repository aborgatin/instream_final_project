package com.github.aborgatin.stream.finalproject.spark.config

import java.util.Objects

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}

@JsonInclude(Include.NON_NULL)
case class Config(
                   @JsonProperty("latency.seconds") private val latency: String,
                   @JsonProperty("bot.requests") private val botRequests: String,
                   @JsonProperty("bot.window.duration") botSeconds: String,
                   @JsonProperty("bot.slide.duration") botSlideSeconds: String,
                   @JsonProperty("bot.whitelist.latency.minutes") private val whitelistLatencyInMinutes: String,
                   @JsonProperty("events.watermark.minutes") eventsWatermark: String,
                   @JsonProperty("spark") sparkParam: SparkParam,
                   @JsonProperty("stream.source") streamSystemParam: DataConnectorParam,
                   @JsonProperty("storage.connector") storageConnectorParam: DataConnectorParam,
                   @JsonProperty("cache.connector") cacheConnectorParam: DataConnectorParam,
                   @JsonProperty("console.writer") consoleWriter: ConsoleWriter
                 ) {

  import Parser._

  def getBotRequests(): Int = strToIntOrDefault(botRequests, 20)

  def getLatency(): Int = strToIntOrDefault(latency, 60)

  def getEventsWatermark(): Long = strToLongOrDefault(eventsWatermark, 10)

  def getWhitelistLatencyInMinutes(): Int = strToIntOrDefault(whitelistLatencyInMinutes, 10)
}

@JsonInclude(Include.NON_NULL)
case class SparkParam(@JsonProperty("log.level") private val logLevel: String,
                      @JsonProperty("app.name") private val applicationName: String,
                      @JsonProperty("app.type") private val applicationType: String,
                      @JsonProperty("checkpoint.dir") private val checkPointDir: String,
                      @JsonProperty("config") private val config: Map[String, String]) {
  def getLogLevel: String = if (logLevel != null) logLevel else "WARN"

  def getConfig: Map[String, String] = if (config != null) config else Map[String, String]()

  def getApplicationName: String = if (applicationName != null) applicationName else "Fraud detector"

  def getApplicationType: ApplicationType = ApplicationType(applicationType)

  def getCheckpointDir: String = if (Objects.nonNull(checkPointDir)) checkPointDir else "/tmp/spark-checkpoint"
}

@JsonInclude(Include.NON_NULL)
case class DataConnectorParam(@JsonProperty("type") sinkType: String,
                              @JsonProperty("format") format: String,
                              @JsonProperty("config") private val config: Map[String, String],
                              @JsonProperty("spark.conf") private val sparkConf: Map[String, String]) {
  def getConfig: Map[String, String] = if (config != null) config else Map[String, String]()

  def getSparkConf: Map[String, String] = if (sparkConf != null) sparkConf else Map[String, String]()
}

@JsonInclude(Include.NON_NULL)
case class StreamParam(@JsonProperty("format") format: String,
                       @JsonProperty("config") private val config: Map[String, String]) {
  def getConfig: Map[String, String] = if (config != null) config else Map[String, String]()
}

case class ConsoleWriter(@JsonProperty("print.events") private val printEvents: String,
                         @JsonProperty("print.events.count") private val printEventsCount: String
                        ) {

  import Parser._


  def isPrintEvents(): Boolean = strToBooleanOrDefault(printEvents, false)

  def getPrintEventsCount(): Int = strToIntOrDefault(printEventsCount, 20)

}

object Parser {

  def strToBooleanOrDefault(booleanString: String, default: Boolean): Boolean = {
    try {
      booleanString.toBoolean
    } catch {
      case e: Exception => default
    }
  }

  def strToIntOrDefault(str: String, default: Int): Int = {
    try {
      str.toInt
    } catch {
      case e: Exception => default
    }
  }

  def strToLongOrDefault(str: String, default: Long): Long = {
    try {
      str.toLong
    } catch {
      case e: Exception => default
    }
  }
}