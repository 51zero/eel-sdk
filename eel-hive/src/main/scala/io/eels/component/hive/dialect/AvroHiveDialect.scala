package io.eels.component.hive.dialect

import java.nio.file.Files

import com.sksamuel.exts.Logging
import io.eels.component.avro.{AvroSourcePart, AvroWriter}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): CloseableIterator[Seq[Row]] = {
    val dest = Files.createTempFile("avro", ".avro")
    dest.toFile.deleteOnExit()
    fs.copyToLocalFile(path, new Path(dest.toAbsolutePath.toString))
    AvroSourcePart(dest).iterator()
  }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = new HiveWriter {

    private val out = fs.create(path)
    private val writer = new AvroWriter(schema, out)

    override def write(row: Row): Unit = writer.write(row)

    override def close(): Unit = {
      writer.close()
      out.close()
    }

    override def records: Int = writer.records
  }
}
