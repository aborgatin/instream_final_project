package com.github.aborgatin.stream.finalproject.spark.app.dstream.window

import java.time.{Duration, LocalDateTime}

case class TimeRangeIterator(startRange: TimeRange, endRange: TimeRange, duration: Duration) extends Iterator[TimeRange] {
  private var current: TimeRange = startRange

  override def hasNext: Boolean = {
    !current.startTime.isAfter(endRange.startTime)
  }

  override def next(): TimeRange = {
    val res = current
    current = TimeRange(current.startTime.plus(duration), current.endTime.plus(duration))
    res
  }

}

case class TimeRange(startTime: LocalDateTime, endTime: LocalDateTime) {
  def between(dateTime: LocalDateTime): Boolean = {
    dateTime.compareTo(startTime) >= 0 && dateTime.compareTo(endTime) <= 0
  }
}
