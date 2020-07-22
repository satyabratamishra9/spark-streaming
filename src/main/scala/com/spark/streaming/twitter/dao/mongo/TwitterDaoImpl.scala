package com.spark.streaming.twitter.dao.mongo


import com.spark.streaming.twitter.utils.AppConfig._
import org.mongodb.scala.bson.collection.immutable.Document
import org.json4s.DefaultFormats
import org.mongodb.scala.{Completed, MongoClient, Observer, SingleObservable}
import org.apache.log4j.Logger


class TwitterDaoImpl(mongoConnection: MongoConnection) extends TwitterDao {

  implicit val jsonDefaultFormats = DefaultFormats
  val logger: Logger = Logger.getLogger(this.getClass)
  val mongoClient: MongoClient = mongoConnection.mongoClient.get

  override def insertIntoMongo(record: Document): Unit = {
    val mongodb = mongoClient.getDatabase(database)
    val tweet = mongodb.getCollection(tweetCollection)
    val insertObservable: SingleObservable[Completed] = tweet.insertOne(record)
    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = logger.debug(s"onNext: $result")
      override def onError(e: Throwable): Unit = logger.error(s"onError: $e")
      override def onComplete(): Unit = logger.info("Message Inserted")
    })
  }
}