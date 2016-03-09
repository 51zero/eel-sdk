package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import io.eels.component.parquet.ParquetLogMute
import io.eels._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

import scala.collection.JavaConverters._

object HiveSource {
  def apply(dbName: String, tableName: String)
           (implicit fs: FileSystem, hive: HiveConf): HiveSource = new HiveSource(dbName, tableName)
}

case class HiveSource(private val dbName: String,
                      private val tableName: String,
                      private val partitionExprs: List[PartitionConstraint] = Nil,
                      private val columns: Seq[String] = Nil)
                     (implicit fs: FileSystem, hive: HiveConf)
  extends Source
    with Logging
    with Using {
  ParquetLogMute()

  def withColumns(columns: Seq[String]): HiveSource = {
    require(columns.nonEmpty)
    copy(columns = columns)
  }

  def withColumns(first: String, rest: String*): HiveSource = withColumns(first +: rest)

  def withPartitionConstraint(name: String, value: String): HiveSource = withPartitionConstraint(PartitionEquals(name, value))

  def withPartitionConstraint(expr: PartitionConstraint): HiveSource = {
    copy(partitionExprs = partitionExprs :+ expr)
  }

  private def createClient: HiveMetaStoreClient = new HiveMetaStoreClient(hive)

  /**
    * Returns all partition values for a given partition key.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(key: String): Set[String] = {
    using(createClient) { client =>
      HiveOps.partitionValues(dbName, tableName, key)(client)
    }
  }

  /**
    * Returns all partition values for a given partition key.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(keys: Seq[String]): Seq[Set[String]] = {
    using(createClient) { client =>
      HiveOps.partitionValues(dbName, tableName, keys)(client)
    }
  }

  def partitionMap: Map[String, Set[String]] = {
    using(createClient) { client =>
      HiveOps.partitionMap(dbName, tableName)(client)
    }
  }

  override def schema: Schema = {
    using(createClient) { client =>
      val lowerColumns = columns.map(_.toLowerCase)
      val s = client.getSchema(dbName, tableName).asScala.filter(fs => columns.isEmpty || lowerColumns.contains(fs.getName.toLowerCase))
      HiveSchemaFns.fromHiveFields(s)
    }
  }

  def spec: HiveSpec = HiveSpecFn.toHiveSpec(dbName, tableName)

  private def dialect(t: Table): HiveDialect = {

    val format = t.getSd.getInputFormat
    logger.debug(s"Table format is $format")

    val dialect = HiveDialect(format)
    logger.debug(s"HiveDialect is $dialect")

    dialect
  }

  override def parts: Seq[Part] = {
    using(createClient) { client =>

      val t = client.getTable(dbName, tableName)
      val schema = this.schema
      val dialect = this.dialect(t)

      val paths = HiveFileScanner(t, partitionExprs)
      paths.map(new DialectPart(_, schema, dialect))
    }
  }

  class DialectPart(path: Path, schema: Schema, dialect: HiveDialect) extends Part {
    override def reader: SourceReader = dialect.reader(path, schema, columns)
  }

}