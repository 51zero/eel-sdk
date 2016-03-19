package io.eels.cli

import java.io.PrintStream

import io.eels.{Constants, SourceParser}
import io.eels.component.hive.{HiveSource, HiveSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object FetchSpecMain {

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  def apply(args: Seq[String], out: PrintStream = System.out): Unit = {

    val parser = new scopt.OptionParser[Options]("eel") {
      head("eel fetch-spec", Constants.EelVersion)

      opt[String]("dataset") required() action { (source, o) =>
        o.copy(source = source)
      } text "specify dataset, eg hive:database:table"
    }

    parser.parse(args, Options()) match {
      case Some(options) =>
        val builder = SourceParser(options.source).getOrElse(sys.error(s"Unsupported source ${options.source}"))
        val source = builder()
        source match {
          case hive: HiveSource =>
            val spec = hive.spec
            val json = HiveSpec.writeAsJson(spec.copy(tables = spec.tables.filter(_.tableName == hive.tableName)))
            println(json)
          case _ =>
            sys.error(s"Unsupported source $source")
        }
      case _ =>
    }
  }

  case class Options(source: String = null)
}

