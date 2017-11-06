package com.jrancier.twitter

import org.apache.spark.rdd.RDD
import twitter4j.{MediaEntity, Status}
import com.jrancier.twitter.emoji.EmojiSource.emojis

object Transforms {

  private val emojiHash = Map(emojis.map(i => i.unified -> i): _*)

  private val emojiR = "[^\u0000-\uFFFF]".r

  private val urlR = "https?://([\\w|\\.]*)".r

  def getTweetDomains(source: RDD[Status]) : RDD[Array[String]] = {
    source.flatMap(status => {
      val urls = status.getURLEntities.map(_.getExpandedURL).flatMap(urlR.findFirstMatchIn(_).map(_.group(1)))
      if(urls.nonEmpty) {
        Some(urls)
      } else {
        None
      }
    })
  }

  def getTweetPics(source: RDD[Status]) : RDD[Array[MediaEntity]] = {
    source.flatMap(status => {
      val pictures = status.getMediaEntities.filter(_.getType == "photo")
      if(pictures.nonEmpty) {
        Some( pictures)
      } else {
        None
      }
    })
  }

  def getTweetHashTags(source: RDD[Status]) : RDD[Array[String]] = {
    source.flatMap(status => {
      val tags = status.getHashtagEntities.map(_.getText)
      if(tags.nonEmpty) {
        Some( tags)
      } else {
        None
      }
    })
  }

  def getTweetEmojis(source: RDD[Status]) : RDD[Array[String]] = {
    source.map(status => {
      val potentialMatches = emojiR.findAllIn(status.getText).toArray
      potentialMatches.flatMap(emojiHash.get).map(_.name)
    }).filter(seq => seq.nonEmpty)
  }

  def getNewOccuranceMap(domains: RDD[Array[String]], existingTally : Map[String, Long]) : Map[String, Long] = {
    val newValues = domains.flatMap(itr => itr.toSeq).countByValue()
    val combinedList =  newValues.toList ++ existingTally.toList
    combinedList.groupBy(_._1).map{case(k, v) => k -> v.map(_._2).sum}
  }
}
