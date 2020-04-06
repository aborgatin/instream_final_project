package com.github.aborgatin.stream.finalproject.ignite

import org.apache.ignite.Ignition

object IgniteNodeStart {
  private val CFG = "/work/projects/spark-streaming-fraud-detector/src/main/resources/config/example-default.xml"
  def main(args: Array[String]): Unit = {
    val ignite = Ignition.start(CFG)
  }
}
