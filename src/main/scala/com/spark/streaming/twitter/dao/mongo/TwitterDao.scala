package com.spark.streaming.twitter.dao.mongo


import com.spark.streaming.twitter.schemas.Schemas.TweetConsumerMessage


trait TwitterDao {
  def insertIntoMongo(record: TweetConsumerMessage): Unit
}
