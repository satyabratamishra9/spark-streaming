package com.spark.streaming.twitter.dao.mongo

import org.mongodb.scala.MongoClient

trait MongoConnection {

  val mongoClient: Option[MongoClient]

  def closeConnection(): Unit = {
    if (mongoClient.isDefined) {
      mongoClient.get.close()
    }
  }
}