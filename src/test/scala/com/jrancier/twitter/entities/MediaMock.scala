package com.jrancier.twitter.entities

import twitter4j.{MediaEntity, URLEntity}

case class MediaMock(`type`: String) extends URLEntity with MediaEntity {
  override def getMediaURL = ???

  override def getMediaURLHttps = ???

  override def getId = ???

  override def getType = `type`

  override def getSizes = ???

  override def getStart = ???

  override def getExpandedURL = ???

  override def getText = ???

  override def getEnd = ???

  override def getURL = ???

  override def getDisplayURL = ???
}
