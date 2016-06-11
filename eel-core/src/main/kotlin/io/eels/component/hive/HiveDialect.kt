package io.eels.component.hive

import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Predicate
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import rx.Observable

interface HiveDialect {

  /**
   * Creates a new reader that will read from the given hadoop path.
   *
   * The dataSchema represents the schema that was written for the data files. This won't necessarily be the same
   * as the hive metastore schema, because partition values are not written to the data files. We must include
   * this here because some hive formats don't store schema information with the data, eg delimited files.
   *
   * The projection is the schema required by the user which may be the same as the written data, or
   * it may be a subset if a projection pushdown is being used.
   *
   */
  fun reader(path: Path, dataSchema: Schema, projection: Schema, predicate: Predicate?, fs: FileSystem): Observable<Row>

  fun writer(schema: Schema, path: Path, fs: FileSystem): HiveWriter
}

interface HiveWriter {
  fun write(row: Row): Unit
  fun close(): Unit
}