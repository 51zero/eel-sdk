package io.eels.component.mongo

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.Frame

object MongoTestApp extends App with StrictLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  val frame = Frame(
    Map("artist" -> "elton", "album" -> "yellow brick road", "year" -> "1972"),
    Map("artist" -> "elton", "album" -> "tumbleweed connection", "year" -> "1974"),
    Map("artist" -> "elton", "album" -> "empty sky", "year" -> "1969"),
    Map("artist" -> "beatles", "album" -> "white album", "year" -> "1696"),
    Map("artist" -> "beatles", "album" -> "tumbleweed connection", "year" -> "1966"),
    Map("artist" -> "pinkfloyd", "album" -> "the wall", "year" -> "1979"),
    Map("artist" -> "pinkfloyd", "album" -> "dark side of the moon", "year" -> "1974"),
    Map("artist" -> "pinkfloyd", "album" -> "emily", "year" -> "1966")
  )

  val sink = MongoSink(MongoSinkConfig("mongodb://localhost:27017", "samdb", "albums2"))
  frame.to(sink)
  logger.info("Write complete")
}
