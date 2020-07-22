package com.spark.streaming.twitter.dao.mongo


import org.mongodb.scala.bson.collection.immutable.Document


trait TwitterDao {
  def insertIntoMongo(record: Document): Unit
}
