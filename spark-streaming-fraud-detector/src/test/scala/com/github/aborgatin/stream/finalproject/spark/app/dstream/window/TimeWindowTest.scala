package com.github.aborgatin.stream.finalproject.spark.app.dstream.window

import java.sql.Timestamp
import java.time.LocalDateTime

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimeWindowTest  extends AnyFlatSpec with Matchers {


  "A TimeWindow with duration 10 sec and slide 5 sec" should "return 2 TimeRanges" in {
    val timeWindow = TimeWindow.window(Timestamp.valueOf(LocalDateTime.of(2020, 4, 3, 16, 4, 4)), "10 seconds", "5 seconds")
    val intervals = timeWindow.toIntervals
    intervals.size should be(2)
  }

  "A TimeWindow with duration 10 sec and slide 1 sec" should "return 11 TimeRanges" in {
    val timeWindow = TimeWindow.window(Timestamp.valueOf(LocalDateTime.now()), "10 seconds", "1 seconds")
    val intervals = timeWindow.toIntervals
    intervals.size should be(11)
  }

}
