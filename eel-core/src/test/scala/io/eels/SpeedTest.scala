package io.eels

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import com.sksamuel.scalax.metrics.Timed
import com.univocity.parsers.csv.CsvParserSettings
import io.eels.component.csv.CsvSource

object SpeedTest extends App with Timed {

  import scala.concurrent.ExecutionContext.Implicits.global

  val path = Paths.get("big.csv")

  //  timed("plain java io") {
  //    val rows = Files.readAllLines(path).asScala
  //    println(rows.last)
  //  }

  //  timed("univocity") {
  //    val settings = new CsvParserSettings()
  //    settings.getFormat.setDelimiter(',')
  //    settings.setDelimiterDetectionEnabled(true)
  //    val count = new AtomicLong(0)
  //    val parser = new com.univocity.parsers.csv.CsvParser(settings)
  //    parser.beginParsing(path.toFile)
  //    Iterator.continually(parser.parseNext).takeWhile(_ != null).foreach { row =>
  //      if (count.getAndIncrement % 1000000 == 0)
  //        println(count.get)
  //    }
  //  }

  timed("eel") {
    val source = CsvSource(path).withDelimiter(',')
    val size = source.size
    println("size=" + size)
  }
}
