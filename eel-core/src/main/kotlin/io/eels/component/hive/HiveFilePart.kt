package io.eels.component.hive

import io.eels.Row
import io.eels.component.Part
import io.eels.component.Predicate
import io.eels.schema.Schema
import io.eels.util.Option
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import rx.Observable

class HiveFilePart(val dialect: HiveDialect,
                   val file: LocatedFileStatus,
                   val schema: Schema,
                   val predicate: Option<Predicate>,
                   val partitionKeys: List<String>,
                   val fs: FileSystem) : Part {

  override fun data(): Observable<Row> {
    return Observable.create { subscriber ->
      // todo use dialect reader to read the rows from the file
      subscriber.onStart()
      subscriber.onNext(Row(schema, schema.fieldNames()))
      subscriber.onNext(Row(schema, schema.fieldNames().reversed()))
      subscriber.onNext(Row(schema, schema.fieldNames()))
      subscriber.onCompleted()
    }
  }

  // the schema we send to the reader must have any partitions removed, because those columns won't exist
  // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
//    val dataSchema = Schema(schema.columns.filterNot { partitionKeys.contains(it.name) })
//    val reader = dialect.reader(file.path, metastoreSchema, dataSchema, predicate, fs)
//
//      // when we read a row back from the dialect reader, we must repopulate any partition columns requested,
//      // because those values are not stored in hive, but inferred from the meta store
//      override fun iterator(): Iterator<Row> = reader.iterator().map { row ->
//        schema.fieldNames().map {
//          // todo add in partition columns
//          // map.getOrElse(it, partition.get(fieldName).orNull)
//          row
//        }
//        row
//      }
}