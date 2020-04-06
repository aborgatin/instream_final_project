package com.github.aborgatin.stream.finalproject.spark.app.dstream

import java.sql.Timestamp

import com.github.aborgatin.stream.finalproject.spark.Loggable
import com.github.aborgatin.stream.finalproject.spark.app.dstream.window.{TimeRange, TimeWindow}
import com.github.aborgatin.stream.finalproject.spark.config.Config
import com.github.aborgatin.stream.finalproject.spark.model.{Bot, UserClickBot}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, State, StateSpec}

case class HelperClasses(private val config: Config) extends Loggable {

  def findBots(stream: DStream[UserClickBot]): DStream[Bot] = {
    stream
      .map(click => click -> TimeWindow.window(click.event_time, config.botSeconds, config.botSlideSeconds).toIntervals)
      .flatMapValues(list => list)
      .map(pair => ((pair._1.ip, pair._2), pair._1))
      .mapWithState {
        StateSpec.function(stateUpdateFunction _)
          .timeout(Durations.minutes(config.getEventsWatermark()))
      }
      .filter(_.isDefined)
      .map(_.get)
      .map(pair => ((pair._1._1, pair._2._2), pair._2._1))
      .filter(_._2 >= config.getBotRequests())
      .map(_._1)
      .reduceByKey(maxTmst)
      .map(pair => Bot(pair._1, pair._2))
  }


  def stateUpdateFunction(key: (String, TimeRange), value: Option[UserClickBot], state: State[(Int, Timestamp)]): Option[((String, TimeRange), (Int, Timestamp))] = {
    (value, state.getOption()) match {
      case (Some(userClickBot), None) =>
        // the 1st visit
        state.update((1, userClickBot.event_time))
        None
      case (Some(userClickBot), Some(pair)) =>
        // next visit
        val c = (pair._1 + 1, maxTmst(pair._2, userClickBot.event_time))

        if (c._1 >= config.getBotRequests()) {
          warn(s"click - pair ${key._1} - ${c._1}")
          state.remove()
          return Some(key, c)
        }
        state.update(c)
        None
      case (None, Some(pair)) => {
        warn(s"None - pair ${key._1} - ${state.get()}")
        Some(key, pair)
      }
      case _ => {
        warn(s"None - None ${key._1} - ${state.get()}")
        None
      }
    }
  }

  def maxTmst(timestamp: Timestamp, secondTimestamp: Timestamp): Timestamp = {
    if (timestamp.after(secondTimestamp)) timestamp else secondTimestamp
  }

  def mapUpdateUserClickAfterJoin(key: (String, (UserClickBot, Option[Timestamp]))): (String, UserClickBot) = {
    val click = key._2._1
    (key._1, UserClickBot(click.ip, click.event_type, click.event_time, click.url, key._2._2.nonEmpty || click.is_bot, click.event_id))
  }
}
