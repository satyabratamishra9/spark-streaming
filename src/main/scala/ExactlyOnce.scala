import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

import Schemas.{Tweet, TweetConsumerMessage}
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.spark
import org.bson.conversions.Bson
//import reactivemongo.api.commands.WriteResult

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
//import scalikejdbc._
//import reactivemongo.api.MongoConnection.ParsedURI
//import reactivemongo.api.collections.bson.BSONCollection
//import reactivemongo.api._
//import reactivemongo.bson.{BSONDocument, BSONDocumentReader}
//import reactivemongo.core.nodeset.Authenticate
//import reactivemongo.api.MongoConnectionOptions

import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, MongoException, Observer}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import java.time.Duration
import scala.concurrent.duration.FiniteDuration



import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await
//import org.mongodb.scala._

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._



object ExactlyOnce {
  var mongoClient: MongoClient = _
  var mongodb: MongoDatabase = _
  var tweet: MongoCollection[Tweet] = _
  lazy val tweetCollection: String = "exampleCollection"

  def getTweetBson(object: Tweet):Bson={

  }

  def main(args: Array[String]): Unit = {
    consumeFromKafka("spark-topic")
  }

  def getMongoConnection(mongoUrl: String): Unit = {
    mongoClient = MongoClient(mongoUrl)
  }

  def consumeFromKafka(topic: String): Unit = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("auto.offset.reset", "latest")
      props.put("group.id", "consumer-group")
      val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
      consumer.subscribe(util.Arrays.asList(topic))
      while (true) {
     //   val record = consumer.poll(1000).asScala
//        for (data <- record.iterator)
//          println(data.value())
//          val x = Tweet(data.value())
          val tweetRecords: ConsumerRecords[String, String] =
          consumer.poll(1000)
   //       insertIntoMongo(getTweetBson(data.value()))
          for (data <- tweetRecords.asScala)
            val x = TweetConsumerMessage(data.value)





      }
//    val brokers = "3.21.127.72:9092,18.217.220.79:9092,3.128.26.238:9092"
//    val brokers = "localhost:9092" 18.222.179.73
//    val topic = "spark-topic"
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> brokers,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "at-least-once",
//      //"enable.auto.commit" -> (false: java.lang.Boolean),
//      "auto.offset.reset" -> "latest")
//
//    val conf = new SparkConf().setAppName("spark-streaming-semantics").setIfMissing("spark.master", "local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val messages = KafkaUtils.createDirectStream[String, String](ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](Seq("spark-topic"), kafkaParams))
//    messages.map(record=> {
//      val data = record.value().toString
//      val document1 = BSONDocument("value" -> data.toString)
//      val writeRes: Future[WriteResult] = getMongoCollection("spark-test","ConsumerData").insert.one(document1)
//      writeRes.onComplete {
//        case Failure(e) => e.printStackTrace()
//        case Success(writeResult) =>
//          println(s"successfully inserted document with result: $writeResult")
//      }
//    }).print
//    ssc.start()
//    ssc.awaitTermination()
  //    getMongoCollection("spark-test","ConsumerData")
//  }
  //def getMongoCollection(dataBase : String, collection : String) : BSONCollection = {
//    def servers: List[String] = List("18.222.232.155:27017", "3.136.108.176:27017", "3.128.197.113:27017")
    // setting up mongo connection, database and collection

    //val servers = List("server1:27017", "server2:27017", "server3:27017")
    //val connection: MongoConnection = driver.connection(servers)
  //  val servers = "18.222.232.155:27017,3.136.108.176:27017,3.128.197.113:27017"
  //  val dbName = "spark-test"
   // val userName = "siteRootAdmin"
   // val password = "passw0rd"
  //  val credentials = List(Authenticate(dbName, userName, password))

    val servers = "localhost:27017"
    val dbName = "driver_pool"
    val userName = "admin1"
    val password = "password1"

    //val driver: MongoDriver = new MongoDriver()
    //val connection: MongoConnection = driver.connection(ParsedURI(hosts =
    //  List(("172.31.1.49", 27017)),

    val uri = s"mongodb://${userName}:${password}@${servers}/${dbName}?retryWrites=true&w=majority"
    println (uri)

      getMongoConnection(uri)

    mongodb = mongoClient.getDatabase(("driver_pool"))
    tweet = mongodb.getCollection(tweetCollection)
    tweet.insertOne()
      //
//    val driver: MongoDriver = new MongoDriver()
//    val connection: Try[MongoConnection] = driver.connection(uri)
//
//    import scala.util.{Success, Failure}
//
//    var con: MongoConnection = null
//
//    connection match {
//      case Success(x) => con = x
//        case Failure(exp) => println(exp)
//    }


    // val connection: MongoConnection = driver.connection(servers, authentications = credentials),
   //   options = MongoConnectionOptions(nbChannelsPerNode = 200, connectTimeoutMS = 5000),
   //   ignoredOptions = List.empty[String], db = None, authenticate = None))
    //Failover Strategy for Mongo Connections

//    val db = Await.result(con.database(dataBase), 20 seconds)
//    val bsonCollection = db.collection[BSONCollection](collection)
//    bsonCollection
 }
}

