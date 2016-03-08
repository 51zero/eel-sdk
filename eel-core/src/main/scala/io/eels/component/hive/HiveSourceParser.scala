package io.eels.component.hive

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.{SinkParser, SourceParser}
import io.eels.component.Builder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object HiveSourceParser extends SourceParser {
  val HiveRegex = "hive:(.+?):(.+?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[HiveSource]] = str match {
    case HiveRegex(db, table, params) =>
      Some(HiveSourceBuilder(db, table, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class HiveSourceBuilder(db: String, table: String, params: Map[String, List[String]])
  extends Builder[HiveSource] {
  require(db != null, "database name cannot be null")
  require(table != null, "table name cannot be null")
  override def apply(): HiveSource = {
    implicit val fs = FileSystem.get(new Configuration)
    implicit val conf = new HiveConf()
    HiveSource(db, table)
  }
}

object HiveSinkParser extends SinkParser {
  val HiveRegex = "hive:(.+?):(.+?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[HiveSink]] = str match {
    case HiveRegex(db, table, params) =>
      Some(HiveSinkBuilder(db, table, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class HiveSinkBuilder(db: String, table: String, params: Map[String, List[String]])
  extends Builder[HiveSink] {
  require(db != null, "database name cannot be null")
  require(table != null, "table name cannot be null")
  override def apply(): HiveSink = {
    implicit val fs = FileSystem.get(new Configuration)
    implicit val conf = new HiveConf()
    val dynamicPartitioning = params.get("dynamicPartitioning").map(_.head.contains("true"))
    val schemaEvolution = params.get("schemaEvolution").map(_.head.contains("true"))
    HiveSink(db, table, 4, dynamicPartitioning, schemaEvolution)
  }
}