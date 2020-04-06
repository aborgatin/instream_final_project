package com.github.aborgatin.stream.finalproject.spark

import com.github.aborgatin.stream.finalproject.spark.app.StreamApp
import com.github.aborgatin.stream.finalproject.spark.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class BotDetector(private val config: Config) {

  def processWithAwait(): Unit = {
    val sparkParam = config.sparkParam
    val storageConnectorParam = config.storageConnectorParam
    val cacheConnectorParam = config.cacheConnectorParam
    val conf = new SparkConf().setAppName(sparkParam.getApplicationName)
    conf.setAll(sparkParam.getConfig ++ storageConnectorParam.getSparkConf ++ cacheConnectorParam.getSparkConf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel(sparkParam.getLogLevel)
    val streamApp: StreamApp = StreamApp(sparkParam.getApplicationType, config, spark)
    streamApp.processWithAwait()
  }

}
