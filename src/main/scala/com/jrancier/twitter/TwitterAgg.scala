package com.jrancier.twitter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import scala.collection.mutable.PriorityQueue

object TwitterAgg {

  def main(args: Array[String]): Unit = {
    if(args.length < 4){
      System.err.println("Usage: TwitterLocations <consumer key> <consumer secret> " +
        "<access token> <access token secret>")
      System.exit(1)
    }

    System.setProperty("twitter4j.oauth.consumerKey", args(0))
    System.setProperty("twitter4j.oauth.consumerSecret", args(1))
    System.setProperty("twitter4j.oauth.accessToken", args(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))

    val sparkConf = new SparkConf().setAppName("TwitterAgg")

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, Nil)

    var runningCount = 0L
    var emojiTweetsCount = 0L
    var urlCount = 0L
    var tweetsWithPicsCount = 0L

    var startTime = System.currentTimeMillis
    def getDelta = System.currentTimeMillis() - startTime

    def getSecondsPassed = getDelta.toDouble / 1000
    def getMinutesPassed = getDelta.toDouble / (1000 * 60)
    def getHoursPassed = getDelta.toDouble / (1000 * 60 * 60)


    var emojiOccuranceHash = Map[String, Long]()
    var domainOcuuranceMap = Map[String, Long]()
    var hashTagOcuuranceMap = Map[String, Long]()

    stream.foreachRDD(rdd => {
      runningCount += rdd.count()

      val tweetsWithUrls = Transforms.getTweetDomains(rdd).cache()
      urlCount += tweetsWithUrls.count()
      domainOcuuranceMap = Transforms.getNewOccuranceMap(tweetsWithUrls, domainOcuuranceMap)
      val domainsQueue = PriorityQueue(domainOcuuranceMap.toSeq.map(tuple => StringOccurance(tuple._1, tuple._2)) : _*)(MaxOrder)

      tweetsWithPicsCount += Transforms.getTweetPics(rdd).count()

      val tweetsWithHashtags = Transforms.getTweetHashTags(rdd).cache()
      hashTagOcuuranceMap = Transforms.getNewOccuranceMap(tweetsWithHashtags, hashTagOcuuranceMap)
      val hashTagsQueue = PriorityQueue(hashTagOcuuranceMap.toSeq.map(tuple => StringOccurance(tuple._1, tuple._2)) : _*)(MaxOrder)

      val tweetEmojis = Transforms.getTweetEmojis(rdd).cache()
      emojiTweetsCount += tweetEmojis.count()
      emojiOccuranceHash = Transforms.getNewOccuranceMap(tweetEmojis, emojiOccuranceHash)
      val emojiQueue = PriorityQueue(emojiOccuranceHash.toSeq.map(tuple => StringOccurance(tuple._1, tuple._2)) : _*)(MaxOrder)


      println("\nTotal tweets received %s ".format(runningCount))
      println("\nAverage per Second %s".format(runningCount.toDouble / getSecondsPassed ))
      println("\nAverage per Minute %s".format(runningCount.toDouble / getMinutesPassed ))
      println("\nAverage per Hour %s".format(runningCount.toDouble / getHoursPassed ))
      println("\nPercentage containing Pictures %3.2f%% ".format((tweetsWithPicsCount.toDouble / runningCount.toDouble) * 100))
      println("\nPercentage containing Emojis %3.2f%% ".format((emojiTweetsCount.toDouble / runningCount.toDouble) * 100))
      println("\nTop Emojis")
      (1 to 10).map(_ => if(emojiQueue.isEmpty) StringOccurance("None", 0 ) else emojiQueue.dequeue())
        .foreach(ec => println("%s:%s".format(ec.count, ec.value)))
      println("\nPercentage containing URLs %3.2f%% ".format((urlCount.toDouble / runningCount.toDouble) * 100))
      println("\nTop Urls")
      (1 to 10).map(_ => if(domainsQueue.isEmpty) StringOccurance("None", 0 ) else domainsQueue.dequeue())
        .foreach(dc => println("%s:%s".format(dc.count, dc.value)))
      println("\nTop Hashtags")
      (1 to 10).map(_ => if(hashTagsQueue.isEmpty) StringOccurance("None", 0 ) else hashTagsQueue.dequeue())
        .foreach(dc => println("%s:%s".format(dc.count, dc.value)))
    })


    Logger.getRootLogger.setLevel(Level.ERROR)
    ssc.start()
    ssc.awaitTermination()
  }

  case class StringOccurance(value: String, var count: Long)

  object MaxOrder extends Ordering[StringOccurance] {
    def compare(x:StringOccurance, y:StringOccurance) = x.count compare y.count
  }

}
