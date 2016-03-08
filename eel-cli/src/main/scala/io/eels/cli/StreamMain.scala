package io.eels.cli

import io.eels.{Sink, SinkParser, SourceParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object StreamMain {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  def apply(args: Seq[String]): Unit = {

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
        val sourceBuilder = SourceParser(options.from).orNull
        val source = sourceBuilder()
        val sinkBuilder = SinkParser(options.to).orNull
        val sink = sinkBuilder()
        val result = source.toFrame(options.sourceIOThreads).to(sink)
        println(s"Completed with $result rows")
      case _ =>
    }
  }
}
