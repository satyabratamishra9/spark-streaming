package com.spark.streaming.twitter.dao.mongo

import com.spark.streaming.twitter.utils.AppConfig
import org.apache.log4j.Logger
import org.mongodb.scala.MongoClient

import scala.util.{Failure, Success, Try}

class MongoConnectionImpl extends MongoConnection {

  val logger: Logger = Logger.getLogger(this.getClass)

  override val mongoClient: Option[MongoClient] = initConnection() match {
    case Left(msg) => None
    case Right(client) => Some(client)
  }

  private def initConnection(): Either[Throwable, MongoClient] = {
    val mongoUrl = AppConfig.mongoUrl
    Try(MongoClient(mongoUrl)) match {
      case Success(mongoClient) => Right(mongoClient)
      case Failure(msg) => Left(msg)
    }
  }
}
