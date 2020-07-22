package com.spark.streaming.twitter.launcher


import com.spark.streaming.twitter.consumer.TwitterConsumer

object AppLauncher {

  def main(args: Array[String]): Unit = {
    new TwitterConsumer().startApp()
  }
}
