package com.jrancier.twitter

import java.util.Date

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
    //Need to find a Way to seralize mock/stub of status. Or create our own status via json?
//    val status1 = new StatusJSONImpl
//    }
//    val status2 = stub[Status]
//    val status3 = stub[Status]
//
//    val url1 = stub[URLEntity]
//    val url2 = stub[URLEntity]
//
//    (status1.getURLEntities _) when() returns(Array(url1))
//    (status2.getURLEntities _) when() returns(Array(url2))
//    (status2.getURLEntities _) when() returns(Array())
//
//    (url1.getExpandedURL _) when() returns("www.test.com")
//    (url2.getExpandedURL _) when() returns("www.twitter.com")
//
//    val rdd = sparkContext.parallelize(Seq(status1, status2, status3))
//
//    val result = Transforms.getTweetDomains(rdd).collect().flatten
//
//    result.length should be (2)
//    result.contains("www.test.com") should be (true)
//    result.contains("www.twitter.com") should be (true)
//
  }

}
