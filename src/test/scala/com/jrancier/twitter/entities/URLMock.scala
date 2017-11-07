package com.jrancier.twitter.entities

import twitter4j.URLEntity

case class URLMock(url: String) extends URLEntity{
  override def getStart = ???

  override def getText = ???

  override def getEnd = ???

  override def getExpandedURL = url

  override def getURL = ???

  override def getDisplayURL = ???
}
