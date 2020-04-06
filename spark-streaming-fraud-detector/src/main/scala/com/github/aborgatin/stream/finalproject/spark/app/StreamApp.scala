package com.github.aborgatin.stream.finalproject.spark.app

import com.github.aborgatin.stream.finalproject.spark.Loggable
import com.github.aborgatin.stream.finalproject.spark.app.dstream.DStreamApp
import com.github.aborgatin.stream.finalproject.spark.app.struct.StructuredStreamApp
import com.github.aborgatin.stream.finalproject.spark.config.{ApplicationType, Config, StructuredStreamingAppType}
import com.github.aborgatin.stream.finalproject.spark.datasource.{DataConnector, GenericDataConnector}
import com.github.aborgatin.stream.finalproject.spark.model.{Bot, UserClickBot}
import org.apache.spark.sql.SparkSession

trait StreamApp extends Loggable {
  def processWithAwait(): Unit
}

object StreamApp {
  def apply(applicationType: ApplicationType,
            config: Config,
            spark: SparkSession
           ): StreamApp = {

    val cacheWriter: DataConnector[Bot] = GenericDataConnector[Bot](config.cacheConnectorParam, config.consoleWriter, spark)
    val storageWriter: DataConnector[UserClickBot] = GenericDataConnector[UserClickBot](config.storageConnectorParam, config.consoleWriter, spark)

    applicationType match {
      case StructuredStreamingAppType => StructuredStreamApp(spark, config, cacheWriter, storageWriter)
      case _ => DStreamApp(spark, config, cacheWriter, storageWriter)
    }
  }
}
