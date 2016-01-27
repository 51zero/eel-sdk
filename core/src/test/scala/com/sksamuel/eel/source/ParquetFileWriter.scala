package com.sksamuel.eel.source

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

object ParquetFileWriter extends App {

//  val schema = AvroSchema[Person]
//
//  val writer = new AvroParquetWriter[GenericRecord](new Path("person.pq"), schema)
//  val record1 = new Record(schema)
//  record1.put("name", "clint eastwood")
//  record1.put("job", "actor")
//  record1.put("location", "carmel")
//  writer.write(record1)
//
//  val record2 = new Record(schema)
//  record2.put("name", "elton john")
//  record2.put("job", "musician")
//  record2.put("location", "pinner")
//  writer.write(record2)
//
//  writer.close()

}


