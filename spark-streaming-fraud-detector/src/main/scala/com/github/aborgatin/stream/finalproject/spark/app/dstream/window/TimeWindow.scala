package com.github.aborgatin.stream.finalproject.spark.app.dstream.window

import java.sql.Timestamp
import java.time.Duration
import java.time.temporal.ChronoUnit

import org.apache.spark.unsafe.types.CalendarInterval

case class TimeWindow(timestamp: Timestamp, windowDuration: Int, slideDuration: Int) {

  def toIntervals: List[TimeRange] = {

    val dateTime = timestamp.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS)
    val endTime = dateTime.minusSeconds(dateTime.getSecond % slideDuration)
    val startTime = endTime.minusSeconds(windowDuration)

    TimeRangeIterator(
      TimeRange(startTime, startTime.plusSeconds(windowDuration)),
      TimeRange(endTime, endTime.plusSeconds(windowDuration)),
      Duration.ofSeconds(slideDuration))
      .filter(_.between(dateTime)).toList
  }


}

object TimeWindow {
  def window(timestamp: Timestamp, windowDuration: String, slideDuration: String): TimeWindow = {
    TimeWindow(timestamp, getIntervalInSeconds(windowDuration), getIntervalInSeconds(slideDuration))
  }

  def getIntervalInSeconds(interval: String): Int = {
    val cal = CalendarInterval.fromCaseInsensitiveString(interval)
    if (cal.months > 0) {
      throw new IllegalArgumentException(
        s"Intervals greater than a month is not supported ($interval).")
    }
    (cal.microseconds / CalendarInterval.MICROS_PER_SECOND).intValue()
  }
}
