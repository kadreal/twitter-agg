package com.jrancier.twitter.entities

import twitter4j.HashtagEntity

case class HashTagMock(text: String) extends HashtagEntity {
  override def getStart = ???

  override def getText = text

  override def getEnd = ???
}
