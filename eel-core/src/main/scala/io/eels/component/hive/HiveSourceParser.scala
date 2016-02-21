package io.eels.component.hive

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.SourceParser
import io.eels.component.SourceBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object HiveSourceParser extends SourceParser[HiveSource] {
  val HiveRegex = "hive:(.+?):(.+?)(\\?.*)?".r
  override def apply(str: String): Option[SourceBuilder[HiveSource]] = str match {
    case HiveRegex(db, table, params) =>
      Some(HiveSourceBuilder(db, table, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class HiveSourceBuilder(db: String, table: String, params: Map[String, List[String]])
  extends SourceBuilder[HiveSource] {
  require(db != null, "database name cannot be null")
  require(table != null, "table name cannot be null")
  override def apply: HiveSource = {
    implicit val fs = FileSystem.get(new Configuration)
    implicit val conf = new HiveConf()
    HiveSource(db, table)
  }
}