package com.spark.streaming.twitter.consumer


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import com.spark.streaming.twitter.utils.KafkaConfig._
import org.apache.log4j.Logger
import com.spark.streaming.twitter.dao.mongo.{MongoConnection, MongoConnectionImpl, TwitterDaoImpl}
import org.mongodb.scala.bson.collection.immutable.Document



class TwitterConsumer {

  val logger: Logger = Logger.getLogger(this.getClass)

  val mongoConnection: MongoConnection = new MongoConnectionImpl

  val twitterDao = new TwitterDaoImpl(mongoConnection)

  val kafkaParams = getKafkaConsumerConfig

  def startApp(): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("TwitterStreaming")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    val topic: String = getTopic
    println(s"#################### $getTopic")
    println("########" + kafkaParams)
    val topics = Array(topic)
    println("############## ")
    println(topics(0))
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value).foreachRDD(rdd => {
      println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      println(rdd.count())
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
