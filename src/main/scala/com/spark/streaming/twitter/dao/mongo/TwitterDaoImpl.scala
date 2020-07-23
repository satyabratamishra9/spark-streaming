package com.spark.streaming.twitter.dao.mongo


import com.spark.streaming.twitter.schemas.Schemas.TweetConsumerMessage
import com.spark.streaming.twitter.utils.AppConfig._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observer, SingleObservable}
import org.apache.log4j.Logger
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.mongodb.scala.bson.codecs.{DEFAULT_CODEC_REGISTRY, Macros}


object TwitterDaoImpl extends TwitterDao {

  val logger: Logger = Logger.getLogger(this.getClass)
  val tweetConsumerMessageCodec: CodecProvider = Macros.createCodecProviderIgnoreNone(classOf[TweetConsumerMessage])
  implicit val codecRegistry: CodecRegistry = fromRegistries(fromProviders(tweetConsumerMessageCodec), DEFAULT_CODEC_REGISTRY)
  lazy val mongoConnection: MongoConnection = new MongoConnectionImpl
  lazy val mongoClient: MongoClient = mongoConnection.mongoClient.get

  override def insertIntoMongo(record: TweetConsumerMessage): Unit = {
    val mongodb = mongoClient.getDatabase(database).withCodecRegistry(codecRegistry)
    val tweet: MongoCollection[TweetConsumerMessage] = mongodb.getCollection(tweetCollection)
    val insertObservable: SingleObservable[Completed] = tweet.insertOne(record)
    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = logger.debug(s"onNext: $result")
      override def onError(e: Throwable): Unit = logger.error(s"onError: $e")
      override def onComplete(): Unit = logger.info("Message Inserted")
    })
  }
}