package io.eels.cli

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object Main extends App {

  implicit val fs = FileSystem.get(new Configuration)
  implicit val hiveConf = new HiveConf

  // the first parameter determines the command to run, just like in git, eg git pull, or in hadoop, eg hadoop fs
  val command = args.head
  val params = args.tail

  command match {
    case "schema" => ShowSchemaMain(params)
    case "stream" => StreamMain(params)
    case "apply-spec" => ApplySpecMain(params)
    case "fetch-spec" => FetchSpecMain(params)
    case other => System.err.println(s"Unknown command $other")
  }
}

case class Options(from: String = "", to: String = "", workerThreads: Int = 1, sourceIOThreads: Int = 1)