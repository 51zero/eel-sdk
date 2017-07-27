//package io.eels.component.hive.dialect
//
//import com.sksamuel.exts.Logging
//import io.eels.component.avro.{AvroSourcePublisher, AvroWriter}
//import io.eels.component.hive.{HiveDialect, HiveOutputStream}
//import io.eels.datastream.{Publisher, Subscriber}
//import io.eels.schema.StructType
//import io.eels.{Predicate, Row}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.permission.FsPermission
//import org.apache.hadoop.fs.{FileSystem, Path}
//
//import scala.math.BigDecimal.RoundingMode.RoundingMode
//
//object AvroHiveDialect extends HiveDialect with Logging {
//
//  override def input(path: Path,
//                     metastoreSchema: StructType,
//                     projectionSchema: StructType,
//                     predicate: Option[Predicate])
//                    (implicit fs: FileSystem, conf: Configuration): Publisher[Seq[Row]] = new Publisher[Seq[Row]] {
//    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = AvroSourcePublisher(path).subscribe(subscriber)
//  }
//
//  override def output(schema: StructType,
//                      path: Path,
//                      permission: Option[FsPermission],
//                      roundingMode: RoundingMode,
//                      metadata: Map[String, String])
//                     (implicit fs: FileSystem, conf: Configuration): HiveOutputStream = {
//    val path_x = path
//    new HiveOutputStream {
//
//      private val out = fs.create(path)
//      private val writer = new AvroWriter(schema, out)
//
//      override def write(row: Row): Unit = writer.write(row)
//
//      override def close(): Unit = {
//        writer.close()
//        out.close()
//      }
//
//      override def records: Int = writer.records
//      override def path: Path = path_x
//    }
//  }
//}
