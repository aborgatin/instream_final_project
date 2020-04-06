package com.github.aborgatin.stream.finalproject.spark.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.aborgatin.stream.finalproject.spark.util.UnixTimestampDeserializer



case class UserClick(ip: String, @JsonProperty("type") event_type: String, @JsonDeserialize(using = classOf[UnixTimestampDeserializer]) event_time: java.sql.Timestamp, url: String)

case class UserClickBot(ip: String, event_type: String, event_time: java.sql.Timestamp, url: String, is_bot: Boolean, event_id: String)

case class UserClickPayload(payload: UserClick)






