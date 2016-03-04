package io.eels.cli

object SchemaMain {

  def apply(args: Seq[String]): Unit = {

    val parser = new scopt.OptionParser[Options]("eel") {
      head("eel schema", CliConstants.Version)

      opt[String]("source") required() action { (source, o) =>
        o.copy(from = source)
      } text "specify source, eg hive:database:table"
    }

    parser.parse(args, Options()) match {
      case Some(options) =>
        val source = SourceFn(options.from)
      case _ =>
    }
  }

  case class Options(from: String = "")
}

