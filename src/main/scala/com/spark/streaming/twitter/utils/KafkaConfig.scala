package com.spark.streaming.twitter.utils

import AppConfig._
import org.apache.kafka.common.serialization.StringDeserializer


object KafkaConfig {

  def getKafkaConsumerConfig: Map[String, Object] = {
    val x: Map[String, Object] = Map(
      "bootstrap.servers" -> conf.getString("kafka.broker"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> conf.getString("kafka.group_id"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    println(x)
    x
  }

  def getTopic: String = conf.getString("kafka.topic")
}
