//package io.eels
//
//import java.nio.file.{Files, Paths}
//import java.util.concurrent.atomic.AtomicLong
//
//import com.univocity.parsers.csv.CsvParserSettings
//
//import scala.collection.JavaConverters._
//
//object SpeedTest extends App with Timed {
//
//  val path = Paths.get("big.csv")
//
//  timed {
//    val rows = Files.readAllLines(path).asScala
//    println(rows.last)
//  }
//
//  //  timed {
//  //    val reader = CSVReader.open(path.toFile)
//  //    val rows = reader.iterator.toList
//  //    println(rows.last)
//  //  }
//  //
//  //  timed {
//  //    val in = Files.newBufferedReader(path)
//  //    val rows = CSVFormat.DEFAULT.parse(in).iterator().asScala.toList
//  //    println(rows.last)
//  //  }
//  //
//  //  timed {
//  //    val mapper = new CsvMapper()
//  //    // important: we need "array wrapping" (see next section) here:
//  //    mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
//  //    val rows = mapper.readerFor(classOf[Array[String]]).readValues(path.toFile).asScala.toList
//  //    println(rows.last)
//  //  }
//
//  timed {
//    val settings = new CsvParserSettings()
//    settings.getFormat.setDelimiter(',')
//    settings.setDelimiterDetectionEnabled(true)
//    val count = new AtomicLong(0)
//    val parser = new com.univocity.parsers.csv.CsvParser(settings)
//    parser.beginParsing(path.toFile)
//    Iterator.continually(parser.parseNext).takeWhile(_ != null).foreach { row =>
//      if (count.getAndIncrement % 1000000 == 0)
//        println(count.get())
//    }
//  }
//}
