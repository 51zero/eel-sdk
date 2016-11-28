package io.eels.component.orc

import io.eels.Row
import io.reactivex.functions.Consumer
import io.reactivex.{Emitter, Flowable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.OrcFile
import org.apache.orc.OrcFile.ReaderOptions

import scala.util.control.NonFatal

class OrcReader(path: Path)(implicit conf: Configuration) {

  def rows(): Flowable[Row] = Flowable.generate(new Consumer[Emitter[Row]] {

    val reader = OrcFile.createReader(path, new ReaderOptions(conf))
    val schema = OrcFns.readSchema(reader.getSchema)
    val batch = reader.getSchema().createRowBatch()
    val rows = reader.rows()

    override def accept(e: Emitter[Row]): Unit = {
      try {
        if (rows.nextBatch(batch)) {
          val cols = batch.cols.map(_.asInstanceOf[BytesColumnVector])
          for (k <- 0 until batch.size) {
            val values = cols.map { col =>
              val bytes = col.vector.head.slice(col.start(k), col.start(k) + col.length(k))
              new String(bytes, "UTF8")
            }
            val row = Row(schema, values.toVector)
            e.onNext(row)
          }
        }
        e.onComplete()
      } catch {
        case NonFatal(t) => e.onError(t)
      }
    }
  })
}
