package com.spark.streaming.twitter.consumer


import com.spark.streaming.twitter.dao.mongo.TwitterDaoImpl._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import com.spark.streaming.twitter.utils.KafkaConfig._
import org.apache.log4j.Logger
import com.spark.streaming.twitter.schemas.Schemas.TweetConsumerMessage
import com.google.gson.Gson


class TwitterConsumer {

  val logger: Logger = Logger.getLogger(this.getClass)

  val kafkaParams = getKafkaConsumerConfig


  def startApp(): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("TwitterStreaming")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    val topic: String = getTopic
    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    try {
      stream.map(record => record.value).foreachRDD(rdd => {
        if (rdd.isEmpty()) {
          println("##############rdd is empty")
        } else {
          logger.debug(rdd)
          val ls = rdd.collect().toList
          ls.filter(_.nonEmpty).foreach(s => insertToMongo(TweetConsumerMessage(s)))
        }
      })
    }
    catch {
      case ex: Exception =>
        logger.error(ex)
    }
    finally {
      streamingContext.start()
      streamingContext.awaitTermination()
      streamingContext.stop()
    }

  }

  def insertToMongo(data: TweetConsumerMessage): Unit = {
    insertIntoMongo(data)
  }
}
