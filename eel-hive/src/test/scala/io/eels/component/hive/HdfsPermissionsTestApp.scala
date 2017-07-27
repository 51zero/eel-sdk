package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.component.hdfs.HdfsSource
import io.eels.component.hive.dialect.ParquetHiveDialect
import io.eels.datastream.DataStream
import io.eels.schema.StructType

import scala.util.Random

object HdfsPermissionsTestApp extends App with Logging {

  import HiveConfig._

  val Database = "sam"
  val Table = "permissions"

  val data = Array(
    Vector("elton", "yellow brick road ", "1972"),
    Vector("elton", "tumbleweed connection", "1974"),
    Vector("elton", "empty sky", "1969"),
    Vector("beatles", "white album", "1969"),
    Vector("beatles", "tumbleweed connection", "1966"),
    Vector("pinkfloyd", "the wall", "1979"),
    Vector("pinkfloyd", "dark side of the moon", "1974"),
    Vector("pinkfloyd", "emily", "1966"),
    Vector("jackbruce", "harmony row", "1970"),
    Vector("jethrotull", "aqualung", "1974")
  )

  val rows = List.fill(100)(data(Random.nextInt(data.length)))
  val frame = DataStream.fromValues(StructType("artist", "album", "year"), rows)

  new HiveOps(client).createTable(
    Database,
    Table,
    frame.schema,
    List("artist"),
    dialect = ParquetHiveDialect(),
    overwrite = false
  )

  val sink = HiveSink(Database, Table).withInheritPermission(true)
  frame.to(sink)
  logger.info("Write complete")

  val permissions = HdfsSource("hdfs://localhost:9000/user/hive/warehouse/sam.db/*").permissions()
  println(permissions)
}
