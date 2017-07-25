package io.eels.component.orc

import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.{Subscription, DataStream, Publisher, Subscriber}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile.ReaderOptions
import org.apache.orc._

import scala.collection.JavaConverters._

object OrcSource {
  def apply(path: Path)(implicit fs: FileSystem, conf: Configuration): OrcSource = apply(FilePattern(path))
  def apply(str: String)(implicit fs: FileSystem, conf: Configuration): OrcSource = apply(FilePattern(str))
}

case class OrcSource(pattern: FilePattern,
                     projection: Seq[String] = Nil,
                     predicate: Option[Predicate] = None)
                    (implicit fs: FileSystem, conf: Configuration) extends Source with Using {

  override def parts(): Seq[Publisher[Seq[Row]]] = pattern.toPaths().map(new OrcPublisher(_, projection, predicate))

  def withPredicate(predicate: Predicate): OrcSource = copy(predicate = predicate.some)
  def withProjection(first: String, rest: String*): OrcSource = withProjection(first +: rest)
  def withProjection(fields: Seq[String]): OrcSource = {
    require(fields.nonEmpty)
    copy(projection = fields.toList)
  }

  override def schema: StructType = {
    val reader = OrcFile.createReader(pattern.toPaths().head, new ReaderOptions(conf).maxLength(1))
    val schema = reader.getSchema
    OrcSchemaFns.fromOrcType(schema).asInstanceOf[StructType]
  }

  private def reader() = {
    val options = new ReaderOptions(conf)
    OrcFile.createReader(pattern.toPaths().head, options)
  }

  def count(): Long = reader().getNumberOfRows
  def statistics(): Seq[ColumnStatistics] = reader().getStatistics.toVector
  def stripes(): Seq[StripeInformation] = reader().getStripes.asScala
  def stripeStatistics(): Seq[StripeStatistics] = reader().getStripeStatistics.asScala
}

class OrcPublisher(path: Path,
                   projection: Seq[String],
                   predicate: Option[Predicate])(implicit conf: Configuration) extends Publisher[Seq[Row]] {

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {
      var running = true
      subscriber.subscribed(new Subscription {
        override def cancel(): Unit = running = false
      })
      val reader = OrcFile.createReader(path, new ReaderOptions(conf))
      val fileSchema = OrcSchemaFns.fromOrcType(reader.getSchema).asInstanceOf[StructType]
      val iterator: Iterator[Row] = OrcBatchIterator(reader, fileSchema, projection, predicate).flatten
      iterator.grouped(DataStream.DefaultBatchSize).takeWhile(_ => running).foreach(subscriber.next)
      subscriber.completed()
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }
}

