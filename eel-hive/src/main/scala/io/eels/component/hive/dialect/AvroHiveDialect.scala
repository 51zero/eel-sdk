//package io.eels.component.hive.dialect
//
//import com.typesafe.config.ConfigFactory
//import com.typesafe.scalalogging.slf4j.StrictLogging
//import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn, ConvertingAvroRecordMarshaller}
//import io.eels.component.hive.{HiveDialect, HiveWriter, Predicate}
//import io.eels.schema.Schema
//import io.eels.{InternalRow, SourceReader}
//import org.apache.avro.file.{DataFileReader, DataFileWriter}
//import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
//import org.apache.avro.{file, generic}
//import org.apache.commons.io.IOUtils
//import org.apache.hadoop.fs.{FileSystem, Path}
//
//object AvroHiveDialect extends HiveDialect with StrictLogging {
//
//  val config = ConfigFactory.load()
//
//  override def writer(schema: Schema, path: Path)
//                     (implicit fs: FileSystem): HiveWriter = {
//    logger.debug(s"Creating avro writer for $path")
//
//    // hive is case insensitive
//    val avroSchema = AvroSchemaFn.toAvro(schema, caseSensitive = false)
//    val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
//    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
//    val out = fs.create(path, false)
//    val writer = dataFileWriter.create(avroSchema, out)
//
//    val marshaller = new ConvertingAvroRecordMarshaller(avroSchema)
//
//    new HiveWriter {
//      override def close(): Unit = writer.close()
//      override def write(row: InternalRow): Unit = {
//        val record = marshaller.toRecord(row)
//        writer.append(record)
//      }
//    }
//  }
//
//  override def reader(path: Path, dataSchema: Schema, requestedSchema: Schema, predicate: Option[Predicate])(implicit fs: FileSystem): SourceReader = {
//    logger.debug(s"Creating avro iterator for $path")
//
//    new SourceReader {
//
//      val in = fs.open(path)
//      val bytes = IOUtils.toByteArray(in)
//      in.close()
//
//      val datumReader = new generic.GenericDatumReader[GenericRecord]()
//      val reader = new DataFileReader[GenericRecord](new file.SeekableByteArrayInput(bytes), datumReader)
//      val avroSchema = AvroSchemaFn.toAvro(requestedSchema)
//      val recordFn = new AvroRecordFn
//
//      override def close(): Unit = ()
//      override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
//        override def hasNext: Boolean = reader.hasNext
//        override def next(): InternalRow = recordFn.fromRecord(reader.next, avroSchema)
//      }
//    }
//  }
//}
