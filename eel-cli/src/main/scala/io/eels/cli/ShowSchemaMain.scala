package io.eels.cli

import java.io.PrintStream

import io.eels.SourceParser
import io.eels.component.avro.AvroSchemaFn
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object ShowSchemaMain {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  def apply(args: Seq[String], out: PrintStream = System.out): Unit = {

    val parser = new scopt.OptionParser[Options]("eel") {
      head("eel schema", CliConstants.Version)

      opt[String]("source") required() action { (source, o) =>
        o.copy(source = source)
      } text "specify source, eg hive:database:table or parquet:/path/to/file"
    }

    parser.parse(args, Options()) match {
      case Some(options) =>
        val builder = SourceParser(options.source).getOrElse(sys.error(s"Unsupported source ${options.source}"))
        val source = builder()
        val schema = source.schema
        val avroSchema = AvroSchemaFn.toAvro(schema)
        out.println(avroSchema)
      case _ =>
    }
  }

  case class Options(source: String = "")
}

