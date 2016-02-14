package io.eels.cli

import io.eels.component.hive.{HiveSource, HiveSink}
import io.eels.{Sink, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object Main extends App {

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  val parser = new scopt.OptionParser[Options]("eel") {
    head("eel", "0.21.x")

    opt[String]("source") required() action { (source, o) =>
      o.copy(from = source)
    } text "specify source, eg hive:database:table"

    opt[String]("sink") required() action { (sink, o) =>
      o.copy(to = sink)
    } text "specify sink, eg hive:database:table"

    opt[Int]("sourceThreads") optional() action { (threads, options) =>
      options.copy(sourceIOThreads = threads)
    } text "number of source io threads, defaults to 1"

    opt[Int]("workerThreads") optional() action { (threads, options) =>
      options.copy(workerThreads = threads)
    } text "number of worker threads, defaults to 1"
  }

  parser.parse(args, Options()) match {
    case Some(options) =>
      val source = SourceFn(options.from)
      val sink = SinkFn(options.to)
      val result = source.toFrame(options.sourceIOThreads).to(sink).runConcurrent(options.workerThreads)
      println(s"Completed with $result rows")
    case _ =>

  }
}

object SinkFn {
  val HiveRegex = "hive:(.*?):(.*?)".r
  def apply(uri: String)(implicit fs: FileSystem, hiveConf: HiveConf): Sink = uri match {
    case HiveRegex(database, table) => HiveSink(database, table)
    case _ => sys.error(s"Unsupported sink $uri")
  }
}

object SourceFn {
  val HiveRegex = "hive:(.*?):(.*?)".r
  def apply(uri: String)(implicit fs: FileSystem, hiveConf: HiveConf): Source = uri match {
    case HiveRegex(database, table) => HiveSource(database, table)
    case _ => sys.error(s"Unsupported source $uri")
  }
}

case class Options(from: String = "", to: String = "", workerThreads: Int = 1, sourceIOThreads: Int = 1)