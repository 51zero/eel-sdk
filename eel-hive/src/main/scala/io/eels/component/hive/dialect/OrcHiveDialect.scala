//package io.eels.component.hive.dialect
//
//import com.sksamuel.exts.Logging
//import io.eels.component.hive.{HiveDialect, HiveOutputStream}
//import io.eels.component.orc.{OrcPublisher, OrcWriteOptions, OrcWriter}
//import io.eels.datastream.{Publisher, Subscriber}
//import io.eels.schema.StructType
//import io.eels.{Predicate, Rec, Row}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.permission.FsPermission
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcOutputFormat, OrcSerde}
//
//import scala.math.BigDecimal.RoundingMode.RoundingMode
//
//case class OrcHiveDialect(options: OrcWriteOptions = OrcWriteOptions()) extends HiveDialect with Logging {
//
//  override val serde: String = ""
//  //classOf[OrcSerde].getCanonicalName
//  override val inputFormat: String = ""
//  // classOf[OrcInputFormat].getCanonicalName
//  override val outputFormat: String = "" //classOf[OrcOutputFormat].getCanonicalName
//
//  override def input(path: Path,
//                     metastoreSchema: StructType,
//                     projectionSchema: StructType,
//                     predicate: Option[Predicate])
//                    (implicit fs: FileSystem, conf: Configuration): Publisher[Seq[Row]] = new Publisher[Seq[Row]] {
//    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
//      new OrcPublisher(path, projectionSchema.fieldNames(), predicate).subscribe(subscriber)
//    }
//  }
//
//  override def output(schema: StructType,
//                      path: Path,
//                      permission: Option[FsPermission],
//                      roundingMode: RoundingMode,
//                      metadata: Map[String, String])(implicit fs: FileSystem, conf: Configuration): HiveOutputStream = {
//
//    val path_x = path
//    val writer = new OrcWriter(path, schema, options)
//
//    new HiveOutputStream {
//
//      override def write(row: Rec): Unit = {
//        require(row.nonEmpty, "Attempting to write an empty row")
//        writer.write(row)
//      }
//
//      override def close(): Unit = {
//        writer.close()
//        permission.foreach(fs.setPermission(path, _))
//      }
//
//      override def records: Int = writer.records
//      override def path: Path = path_x
//    }
//  }
//}