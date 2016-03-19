package io.eels.cli

import java.io.PrintStream
import java.nio.file.{Path, Paths}

import io.eels.{Constants, SourceParser}
import io.eels.component.hive.{HiveOps, HiveSource, HiveSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

object ApplySpecMain {

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf
  implicit val client = new HiveMetaStoreClient(hiveConf)

  def apply(args: Seq[String], out: PrintStream = System.out): Unit = {

    val parser = new scopt.OptionParser[Options]("eel") {
      head("eel apply-spec", Constants.EelVersion)

      opt[String]("dataset") required() action { (source, o) =>
        o.copy(source = source)
      } text "specify dataset, eg hive:database:table"

      opt[String]("spec") required() action { (schema, o) =>
        o.copy(specPath = Paths.get(schema))
      } text "specify path to eel spec"
    }

    parser.parse(args, Options()) match {
      case Some(options) =>
        val builder = SourceParser(options.source).getOrElse(sys.error(s"Unsupported source ${options.source}"))
        val source = builder()
        source match {
          case hive: HiveSource =>
            HiveOps.applySpec(HiveSpec(options.specPath), false)
          case _ =>
            sys.error(s"Unsupported source $source")
        }
      case _ =>
    }
  }

  case class Options(source: String = null, specPath: Path = null)
}

