package Schemas

object Schemas {
    def main(args: Array[String]): Unit = {
        println("x")
    }
    case class Tweet(userId: Int, source: String)
    case class TweetConsumerMessage(data: String)
}
