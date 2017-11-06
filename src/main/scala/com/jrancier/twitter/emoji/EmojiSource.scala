package com.jrancier.twitter.emoji

import java.io.InputStream

import scala.io.Source
import scala.util.parsing.json.JSON

object EmojiSource {
  val emojiStream : InputStream = getClass.getResourceAsStream("/emoji.json")
  val lines = Source.fromInputStream( emojiStream ).getLines
  val jsonString = lines.mkString

  val emojis = for {
    Some(L(list)) <- List(JSON.parseFull(jsonString))
    M(emojiData) <- list
    S(name) <- emojiData.get("name")
    S(unified) <- emojiData.get("unified")
    split = unified.split('-')
    unicode = split.map(Integer.parseInt(_, 16)).flatMap(Character.toChars(_)).mkString
  } yield {
    EmojiEntry(name, unicode)
  }
}

class Extractor[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }

object M extends Extractor[Map[String, Any]]
object L extends Extractor[List[Any]]
object S extends Extractor[String]
object D extends Extractor[Double]
object B extends Extractor[Boolean]



case class EmojiEntry(name: String, unified: String)
