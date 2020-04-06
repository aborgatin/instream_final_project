package com.github.aborgatin.stream.finalproject.spark.app.dstream


import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.Instant

import com.github.aborgatin.stream.finalproject.spark.config.Config
import com.github.aborgatin.stream.finalproject.spark.model.{Bot, UserClickBot}
import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class DStreamAppMapWithStateTest extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {
  val dataQueue: mutable.Queue[RDD[UserClickBot]] = new mutable.Queue[RDD[UserClickBot]]()
  val config = Config("10", "5", "5 seconds", "1 second", "10", "10", null, null, null, null, null)
  val helper = HelperClasses(config)

  ignore  should "return 2 TimeRanges" in {
    val events = Seq(
      UserClickBot("192.168.0.1", "click", Timestamp.from(Instant.now()), "home.html", false, "rand1"),
      UserClickBot("192.168.0.1", "click", Timestamp.from(Instant.now()), "home.html", false, "rand2"),
      UserClickBot("192.168.0.1", "click", Timestamp.from(Instant.now()), "home.html", false, "rand3"),
      UserClickBot("192.168.0.1", "click", Timestamp.from(Instant.now()), "home.html", false, "rand4"),
      UserClickBot("192.168.0.1", "click", Timestamp.from(Instant.now()), "home.html", false, "rand5")
    )
    val sessionsAccumulator = streamingContext.sparkContext.collectionAccumulator[Bot]("bots")

    events.foreach(event => dataQueue += streamingContext.sparkContext.makeRDD(Seq(event)))
    val testDStream = streamingContext.queueStream(dataQueue)
    helper.findBots(testDStream)
      .foreachRDD( rdd => {
        val terminatedSessions =
          rdd.collect()
        terminatedSessions.foreach(sessionsAccumulator.add)
      }
      )
    Files.deleteIfExists(Paths.get("./spark/checkpoint/*"))
    streamingContext.checkpoint("./spark/checkpoint")

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(6000)
    println(s"Terminated sessions are ${sessionsAccumulator.value}")
    sessionsAccumulator.value.size shouldEqual(1)
  }

}
