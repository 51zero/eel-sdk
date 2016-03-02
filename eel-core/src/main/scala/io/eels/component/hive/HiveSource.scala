package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.parquet.ParquetLogMute
import io.eels._
import org.apache.hadoop.fs.FileSystem
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
                      private val partitionExprs: List[PartitionExpr] = Nil,
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

  def withPartitionConstraint(name: String, value: String): HiveSource = withPartitionConstraint(name, "=", value)
  def withPartitionConstraint(name: String, op: String, value: String): HiveSource = {
    val expr = op match {
      case "=" => PartitionEquals(name, value)
      case ">" => PartitionGt(name, value)
      case ">=" => PartitionGte(name, value)
      case "<" => PartitionLt(name, value)
      case "<=" => PartitionLte(name, value)
      case _ => sys.error(s"Unsupported op $op")
    }
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
      val s = client.getSchema(dbName, tableName).asScala.filter(fs => columns.isEmpty || columns.contains(fs.getName))
      HiveSchemaFns.fromHiveFields(s)
    }
  }

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

      // check the metastore to see if all requests columns are partition columns, if so,
      // then we can just load directly from the metastore
      val partitions = HiveOps.partitions(dbName, tableName)(client)
      val partitionKeys = partitions.flatMap(_.parts.map(_.key)).toSet
      if (columns.nonEmpty && columns.forall(partitionKeys.contains)) {
        logger.debug("Requested columns are all partitioned - reading directly from metastore")
        val part = new Part {
          override def reader = new SourceReader {
            override def close(): Unit = ()
            // we only want to keep the requested columns
            override def iterator: Iterator[InternalRow] = partitions.iterator
              .filter { partition => partitionExprs.isEmpty || partitionExprs.exists(_.eval(partition.parts)) }
              .map { partition => partition.parts.filter(columns contains _.key).map(_.value) }
          }
        }
        Seq(part)
      } else {
        val paths = HiveFileScanner(t, partitionExprs)
        paths.map { path =>
          new Part {
            override def reader = new SourceReader {
              ParquetLogMute()
              lazy val iterator = dialect.iterator(path, schema, columns)
              override def close(): Unit = {
                logger.debug("Closing hive reader")
                // todo close dialect
              }
            }
          }
        }
      }
    }
  }
}