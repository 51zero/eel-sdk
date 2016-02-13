package io.eels.component.hive

import java.util.UUID

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

case class HiveSink(dbName: String, tableName: String, props: HiveSinkProps = HiveSinkProps())
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {
  override def writer: Writer = new Writer {

    implicit val client = new HiveMetaStoreClient(hiveConf)

    var created = false

    def ensureTableCreated(row: Row): Unit = {
      logger.debug(s"Ensuring table $tableName is created")
      if (props.createTable && !created) {
        HiveOps.createTable(dbName, tableName, FrameSchema(row.columns), props.overwriteTable)
        created = true
      }
    }

    def createOutputStream: FSDataOutputStream = {
      val path = new Path(HiveOps.location(dbName, tableName), UUID.randomUUID.toString)
      logger.debug(s"Hive sink will write to $path")
      fs.create(path, false)
    }

    lazy val dialect = {
      val format = HiveOps.tableFormat(dbName, tableName)
      logger.debug(s"Table format is $format")
      HiveDialect(format)
    }

    var out: FSDataOutputStream = _

    override def close(): Unit = if (out != null) out.close()

    override def write(row: Row): Unit = {
      ensureTableCreated(row)
      if (out == null)
        out = createOutputStream
      dialect.write(row, out)
    }
  }
}

case class HiveSinkProps(createTable: Boolean = false, overwriteTable: Boolean = false)