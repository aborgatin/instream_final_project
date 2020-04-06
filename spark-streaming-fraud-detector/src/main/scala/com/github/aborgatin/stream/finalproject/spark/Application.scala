package com.github.aborgatin.stream.finalproject.spark

import com.github.aborgatin.stream.finalproject.spark.config.ConfigReader

case object Application extends Loggable {


  def main(args: Array[String]): Unit = {

    info(args.toString)
    val configPath =
      if (args.length > 0) {
        args(0)
      } else {
        null
      }

    val config = ConfigReader.readConfig(configPath)

    val botDetector = BotDetector(config)

    botDetector.processWithAwait()
  }

}
