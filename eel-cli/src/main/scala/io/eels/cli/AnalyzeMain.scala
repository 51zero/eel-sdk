package io.eels.cli

import java.io.PrintStream

import io.eels.{Constants, SourceParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object AnalyzeMain {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  def apply(args: Seq[String], out: PrintStream = System.out): Unit = {

    val parser = new scopt.OptionParser[Options]("eel") {
      head("eel analyze", Constants.EelVersion)

      opt[String]("dataset") required() action { (source, o) =>
        o.copy(source = source)
      } text "specify dataset, eg hive:database:table"

      opt[Boolean]("reverse") optional() action { (reverse, o) =>
        o.copy(reverse = reverse)
      } text "specify reverse ordering of columns, eg most distinct first"
    }

    parser.parse(args, Options()) match {
      case Some(options) =>
        val builder = SourceParser(options.source).getOrElse(sys.error(s"Unsupported source ${options.source}"))
        val result = builder().counts.toSeq.sortBy(_._2.size)
        val orderedResults = if (options.reverse) result.reverse else result
        for ((columnName, columnCounts) <- orderedResults) {
          println(columnName)
          for ((value, counts) <- columnCounts) {
            println(s"\t$value ($counts)")
          }
        }
      case _ =>
    }
  }

  case class Options(source: String = null, reverse: Boolean = false)
}

