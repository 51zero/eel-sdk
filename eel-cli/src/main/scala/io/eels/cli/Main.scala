package io.eels.cli

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.component.hive.{HiveSource, HiveSink}
import io.eels.{Sink, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object Main extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  // the first parameter determines the command to run, just like in git, eg git pull, or in hadoop, eg hadoop fs
  val command = args.head
  val params = args.tail

  command match {
    case "schema" => SchemaMain(params)
    case "stream" => StreamMain(params)
    case other => System.err.println(s"Unknown command $other")
  }
}

@deprecated("will use the new source and sink parsers", "0.33.0")
object SinkFn {
  val HiveRegex = "hive:(.*?):(.*?)(\\?.*?)?".r
  def apply(uri: String)(implicit fs: FileSystem, hiveConf: HiveConf): Sink = uri match {
    case HiveRegex(database, table, options) =>
      val params = Option(options).map(UrlParamParser.apply).getOrElse(Map.empty)
      HiveSink(database, table, params)
    case _ =>
      sys.error(s"Unsupported sink $uri")
  }
}

@deprecated("will use the new source and sink parsers", "0.33.0")
object SourceFn {
  val HiveRegex = "hive:(.*?):(.*?)".r
  def apply(uri: String)(implicit fs: FileSystem, hiveConf: HiveConf): Source = uri match {
    case HiveRegex(database, table) => HiveSource(database, table)
    case _ => sys.error(s"Unsupported source $uri")
  }
}

case class Options(from: String = "", to: String = "", workerThreads: Int = 1, sourceIOThreads: Int = 1)