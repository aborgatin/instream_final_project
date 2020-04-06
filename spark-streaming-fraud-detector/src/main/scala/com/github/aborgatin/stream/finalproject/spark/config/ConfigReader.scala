package com.github.aborgatin.stream.finalproject.spark.config

import java.io.InputStream

import com.github.aborgatin.stream.finalproject.spark.util.JsonUtil

import scala.io.Source

object ConfigReader {

  def readConfig(path: String): Config = {
    val source = if (path != null) {
      Source.fromFile(path)
    } else {
      val stream: InputStream = getClass.getResourceAsStream("/config.json")
      Source.fromInputStream(stream)
    }
    val param = JsonUtil.fromJson[Config](source.mkString)
    source.close
    param
  }
}
