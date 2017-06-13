package io.eels.component.parquet

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.component.parquet.util.ParquetIterator
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import com.sksamuel.exts.OptionImplicits._
import reactor.core.Disposable
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

class ParquetPart(path: Path,
                  predicate: Option[Predicate],
                  projection: Seq[String])
                 (implicit conf: Configuration) extends Part with Logging with Using {

  lazy val projectionSchema = {
    if (projection.isEmpty)
      None
    else {
      val messageType = ParquetFileReader.open(conf, path).getFileMetaData.getSchema
      val structType = ParquetSchemaFns.fromParquetMessageType(messageType)
      val projected = StructType(structType.fields.filter(field => projection.contains(field.name)))
      ParquetSchemaFns.toParquetMessageType(projected).some
    }
  }

  override def flux(): Flux[Row] = {
    Flux.create(new Consumer[FluxSink[Row]] {
      override def accept(t: FluxSink[Row]): Unit = {

        val reader = RowParquetReaderFn(path, predicate, projectionSchema)

        t.onDispose(new Disposable {
          override def dispose(): Unit = reader.close()
        })

        try {
          ParquetIterator(reader).foreach(t.next)
          t.complete()
        } catch {
          case NonFatal(e) => t.error(e)
        } finally {
          reader.close()
        }
      }
    })
  }

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    val reader = RowParquetReaderFn(path, predicate, projectionSchema)

    override def close(): Unit = {
      super.close()
      reader.close()
    }

    override val iterator: Iterator[Seq[Row]] = ParquetIterator(reader).grouped(100).withPartial(true)
  }
}