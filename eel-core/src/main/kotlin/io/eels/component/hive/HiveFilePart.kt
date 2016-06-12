package io.eels.component.hive

import io.eels.schema.Partition
import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Part
import io.eels.component.Predicate
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import rx.Observable
import rx.Subscription

class HiveFilePart(val dialect: HiveDialect,
                   val file: LocatedFileStatus,
                   val partition: Partition,
                   val metastoreSchema: Schema,
                   val schema: Schema,
                   val predicate: Predicate?,
                   val partitionKeys: List<String>,
                   val fs: FileSystem) : Part {

  override fun data(): Observable<Row> {
    return Observable.create {
      it.add(object : Subscription {
        override fun isUnsubscribed(): Boolean {
          throw UnsupportedOperationException()
        }

        override fun unsubscribe() {
          throw UnsupportedOperationException()
        }
      })
      it.onStart()

      it.onCompleted()
    }
//
//    val obs = data()
//    val sub = obs.subscribe(object : Subscriber<Row>() {
//    })
//    sub.unsubscribe()
  }

  // the schema we send to the reader must have any partitions removed, because those columns won't exist
  // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
//    val dataSchema = Schema(schema.columns.filterNot { partitionKeys.contains(it.name) })
//    val reader = dialect.reader(file.path, metastoreSchema, dataSchema, predicate, fs)
//
//      // when we read a row back from the dialect reader, we must repopulate any partition columns requested,
//      // because those values are not stored in hive, but inferred from the meta store
//      override fun iterator(): Iterator<Row> = reader.iterator().map { row ->
//        schema.columnNames().map {
//          // todo add in partition columns
//          // map.getOrElse(it, partition.get(columnName).orNull)
//          row
//        }
//        row
//      }
}