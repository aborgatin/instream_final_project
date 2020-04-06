package com.github.aborgatin.stream.finalproject.spark.datasource

import com.github.aborgatin.stream.finalproject.spark.config.{ConsoleWriter, DataConnectorParam}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

sealed trait DataConnector[T] {
  def write(ds: Dataset[T]): Unit

  def getDataFrame(): DataFrame
}

case class GenericDataConnector[T](private val dataConnectorParam: DataConnectorParam,
                                   private val consoleWriter: ConsoleWriter,
                                   private val spark: SparkSession
                                  ) extends DataConnector[T] {

  override def write(ds: Dataset[T]): Unit = {

    ds.write
      .format(dataConnectorParam.format)
      .options(dataConnectorParam.getConfig)
      .mode(SaveMode.Append)
      .save()

    if (consoleWriter.isPrintEvents()) ds.show(consoleWriter.getPrintEventsCount(), false)
  }

  override def getDataFrame(): DataFrame = {
    spark.read
      .format(dataConnectorParam.format)
      .options(dataConnectorParam.getConfig)
      .load()
      .where("last_event_time >= cast(current_timestamp as TIMESTAMP) - INTERVAL 10 minutes")
  }
}

