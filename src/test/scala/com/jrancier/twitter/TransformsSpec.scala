package com.jrancier.twitter

import java.util.Date

import com.jrancier.twitter.entities.{HashTagMock, MediaMock, URLMock}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalamock.scalatest.MockFactory
import twitter4j.Status

class TransformsSpec extends FlatSpec with Matchers with MockFactory with BeforeAndAfterAll {

  private var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    sparkContext = new SparkContext(conf)
  }

  def sparkConfig: Map[String, String] = Map.empty

  override def afterAll(): Unit = {
    if (sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
    }
    super.afterAll()
  }


  "getTweetDomains" should "return all domain items" in {

    val url1 = URLMock("https://www.test.com")
    val url2 = URLMock("https://www.twitter.com")

    val status1 = StatusMock(Array(url1), Array(), Array())
    val status2 = StatusMock(Array(url2), Array(), Array())
    val status3 = StatusMock(Array(), Array(), Array())

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetDomains(rdd).collect().flatten

    result.length should be (2)
    result.contains("www.test.com") should be (true)
    result.contains("www.twitter.com") should be (true)
  }

  "getTweetDomains" should "return all domain items even with multiple domains in one tweet" in {

    val url1 = URLMock("https://www.test.com")
    val url2 = URLMock("https://www.twitter.com")

    val status1 = StatusMock(Array(url1, url2), Array(), Array())
    val status2 = StatusMock(Array(), Array(), Array())
    val status3 = StatusMock(Array(), Array(), Array())

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetDomains(rdd).collect().flatten

    result.length should be (2)
    result.contains("www.test.com") should be (true)
    result.contains("www.twitter.com") should be (true)
  }

  "getTweetDomains" should "handle empty rdds" in {

    val rdd = sparkContext.parallelize(Seq()).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetDomains(rdd).collect().flatten

    result.length should be (0)
  }

  "getTweetPics" should "return all media items that are photos" in {

    val pic1 = MediaMock("photo")
    val pic2 = MediaMock("photo")
    val other1 = MediaMock("other")
    val other2 = MediaMock("video")

    val status1 = StatusMock(Array(), Array(), Array(pic1, other1))
    val status2 = StatusMock(Array(), Array(), Array(pic2))
    val status3 = StatusMock(Array(), Array(), Array(other2))

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetPics(rdd).collect().flatten

    result.length should be (2)
    result.contains(pic1) should be (true)
    result.contains(pic2) should be (true)
  }

  "getTweetPics" should "return all media items that are photos even with more then 1 in a tweet" in {

    val pic1 = MediaMock("photo")
    val pic2 = MediaMock("photo")
    val other1 = MediaMock("other")
    val other2 = MediaMock("video")

    val status1 = StatusMock(Array(), Array(), Array(pic1, pic2))
    val status2 = StatusMock(Array(), Array(), Array())
    val status3 = StatusMock(Array(), Array(), Array(other1, other2))

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetPics(rdd).collect().flatten

    result.length should be (2)
    result.contains(pic1) should be (true)
    result.contains(pic2) should be (true)
  }

  "getTweetPics" should "handle an empty RDD stream" in {

    val rdd = sparkContext.parallelize(Seq()).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetPics(rdd).collect().flatten

    result.length should be (0)
  }

  "getTweetHashTags" should "return all hashtags in tweets" in {

    val tag1 = HashTagMock("spark")
    val tag2 = HashTagMock("bahir")
    val tag3 = HashTagMock("twitter")
    val tag4 = HashTagMock("video")

    val status1 = StatusMock(Array(), Array(tag1, tag2), Array())
    val status2 = StatusMock(Array(), Array(tag3), Array())
    val status3 = StatusMock(Array(), Array(tag4), Array())

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetHashTags(rdd).collect().flatten

    result.length should be (4)
    result.contains("spark") should be (true)
    result.contains("bahir") should be (true)
    result.contains("twitter") should be (true)
    result.contains("video") should be (true)
  }

  "getTweetHashTags" should "handle all hashtags in one tweet" in {

    val tag1 = HashTagMock("spark")
    val tag2 = HashTagMock("bahir")
    val tag3 = HashTagMock("twitter")
    val tag4 = HashTagMock("video")

    val status1 = StatusMock(Array(), Array(tag1, tag2, tag3, tag4), Array())
    val status2 = StatusMock(Array(), Array(), Array())
    val status3 = StatusMock(Array(), Array(), Array())

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetHashTags(rdd).collect().flatten

    result.length should be (4)
    result.contains("spark") should be (true)
    result.contains("bahir") should be (true)
    result.contains("twitter") should be (true)
    result.contains("video") should be (true)
  }

  "getTweetHashTags" should "handle empty RDD stream" in {

    val rdd = sparkContext.parallelize(Seq()).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetHashTags(rdd).collect().flatten

    result.length should be (0)
  }

  "getTweetEmojis" should "find all emojis" in {
    val string1 = "Both hands will now be able to interact in virtual reality \uD83D\uDC4C\nIntended application: 3D modeling and animation."
    val string2 = "\uD83D\uDC6F at the start"
    val string3 = "double at the end \uD83D\uDC36\uD83D\uDC95"


    val status1 = StatusMock(Array(), Array(), Array(), string1)
    val status2 = StatusMock(Array(), Array(), Array(), string2)
    val status3 = StatusMock(Array(), Array(), Array(), string3)

    val rdd = sparkContext.parallelize(Seq(status1, status2, status3)).asInstanceOf[RDD[Status]]

    val result = Transforms.getTweetEmojis(rdd).collect().flatten

    result.length should be (4)
    result.contains("DOG FACE") should be (true)
    result.contains("TWO HEARTS") should be (true)
    result.contains("WOMAN WITH BUNNY EARS") should be (true)
    result.contains("OK HAND SIGN") should be (true)
  }

  "getNewOccuranceMap" should "semigroup combined results with existing map" in {
    val startingMap = Map("test" -> 1L, "hello" -> 10L)

    val string1 = "foo"
    val string2 = "bar"
    val string3 = "test"

    val rdd = sparkContext.parallelize(Seq(Array(string1, string2, string3), Array(string2, string3)))

    val result = Transforms.getNewOccuranceMap(rdd, startingMap)

    result.get("test") should be (Some(3L))
    result.get("foo") should be (Some(1L))
    result.get("bar") should be (Some(2L))
    result.get("hello") should be (Some(10L))

  }

  "getNewOccuranceMap" should "works with empty starting map" in {
    val startingMap : Map[String, Long] = Map()

    val string1 = "foo"
    val string2 = "bar"
    val string3 = "test"

    val rdd = sparkContext.parallelize(Seq(Array(string1, string2, string3), Array(string2, string3)))

    val result = Transforms.getNewOccuranceMap(rdd, startingMap)

    result.get("test") should be (Some(2L))
    result.get("foo") should be (Some(1L))
    result.get("bar") should be (Some(2L))

  }

  "getNewOccuranceMap" should "remain the same if no data" in {
    val startingMap = Map("test" -> 1L, "hello" -> 10L)

    val string1 = "foo"
    val string2 = "bar"
    val string3 = "test"

    val rdd = sparkContext.parallelize(Seq(Array[String](), Array[String]()))

    val result = Transforms.getNewOccuranceMap(rdd, startingMap)

    result.get("test") should be (Some(1L))
    result.get("hello") should be (Some(10L))

  }
}
