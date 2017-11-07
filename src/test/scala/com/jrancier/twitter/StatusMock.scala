package com.jrancier.twitter

import twitter4j.{HashtagEntity, MediaEntity, Status, URLEntity}

case class StatusMock(
                       urlEntities : Array[URLEntity],
                       hashTags: Array[HashtagEntity],
                       mediaEntities : Array[MediaEntity],
                       text : String = "") extends Status {

  override def getWithheldInCountries = ???

  override def getQuotedStatus = ???

  override def getUser = ???

  override def getId = ???

  override def getCreatedAt = ???

  override def getFavoriteCount = ???

  override def isPossiblySensitive = ???

  override def getContributors = ???

  override def getGeoLocation = ???

  override def getCurrentUserRetweetId = ???

  override def getInReplyToStatusId = ???

  override def getQuotedStatusId = ???

  override def isRetweeted = ???

  override def isTruncated = ???

  override def getRetweetedStatus = ???

  override def getLang = ???

  override def getRetweetCount = ???

  override def getText = text

  override def isFavorited = ???

  override def getPlace = ???

  override def getScopes = ???

  override def getInReplyToUserId = ???

  override def isRetweetedByMe = ???

  override def getSource = ???

  override def getInReplyToScreenName = ???

  override def isRetweet = ???

  override def getExtendedMediaEntities = ???

  override def getHashtagEntities = hashTags

  override def getURLEntities = urlEntities

  override def getSymbolEntities = ???

  override def getUserMentionEntities = ???

  override def getMediaEntities = mediaEntities

  override def getRateLimitStatus = ???

  override def getAccessLevel = ???

  override def compareTo(o: Status) = ???
}
