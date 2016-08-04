package io.eels.component.parquet

import io.eels.FilePattern
import io.eels.Row
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import io.eels.component.avro.AvroSchemaFns
import io.eels.component.avro.AvroSchemaMerge
import io.eels.schema.Schema
import io.eels.util.Logging
import io.eels.util.Option
import io.eels.util.getOrElse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.ParquetFileReader
import rx.Observable

data class ParquetSource @JvmOverloads constructor(val pattern: FilePattern,
                                                   val fs: FileSystem = FileSystem.get(Configuration()),
                                                   val configuration: Configuration = Configuration()) : Source, Logging, Using {

  @JvmOverloads constructor(path: java.nio.file.Path,
                            fs: FileSystem = FileSystem.get(Configuration()),
                            configuration: Configuration = Configuration()) : this(FilePattern(path, fs), fs, configuration)

  @JvmOverloads constructor(path: Path,
                            fs: FileSystem = FileSystem.get(Configuration()),
                            configuration: Configuration = Configuration()) : this(FilePattern(path, fs), fs, configuration)

  // the schema returned by the parquet source should be a merged version of the
  // schemas contained in all the files.
  override fun schema(): Schema {
    val paths = pattern.toPaths()
    val schemas = paths.map { path ->
      using(ParquetReaderFns.createReader(path, Option.None, Option.None)) { reader ->
        Option(reader.read()).getOrElse { error("Cannot read $path for schema; file contains no records") }.schema
      }
    }
    val avroSchema = AvroSchemaMerge("record", "namspace", schemas)
    return AvroSchemaFns.fromAvroSchema(avroSchema)
  }

  override fun parts(): List<Part> {
    val paths = pattern.toPaths()
    logger.debug("Parquet source will read from $paths")
    val schema = schema()
    return paths.map { ParquetPart(it, schema) }
  }

  fun footers(): List<Footer> {
    val paths = pattern.toPaths()
    logger.debug("Parquet source will read from $paths")
    return paths.flatMap {
      val status = fs.getFileStatus(it)
      logger.debug("status=$status; path=$it")
      ParquetFileReader.readAllFootersInParallel(configuration, status)
    }
  }

  class ParquetPart(val path: Path, val schema: Schema) : Part {
    override fun data(): Observable<Row> = Observable.create { sub ->
      try {
        sub.onStart()
        val reader = ParquetReaderFns.createReader(path, Option.None, Option.None)
        ParquetRowIterator(reader).forEach {
          sub.onNext(it)
        }
      } catch (t: Throwable) {
        sub.onError(t)
      }
      if (!sub.isUnsubscribed)
        sub.onCompleted()
    }
  }
}
