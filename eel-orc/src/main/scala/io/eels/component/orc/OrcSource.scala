package io.eels.component.orc

import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile.ReaderOptions
import org.apache.orc._

import scala.collection.JavaConverters._

case class OrcSource(path: Path,
                     projection: Seq[String] = Nil,
                     predicate: Option[Predicate] = None)(implicit conf: Configuration) extends Source with Using {

  override def parts(): List[Part] = List(new OrcPart(path, projection, predicate))

  def withPredicate(predicate: Predicate): OrcSource = copy(predicate = predicate.some)
  def withProjection(first: String, rest: String*): OrcSource = withProjection(first +: rest)
  def withProjection(fields: Seq[String]): OrcSource = {
    require(fields.nonEmpty)
    copy(projection = fields.toList)
  }

  override def schema: StructType = {
    val reader = OrcFile.createReader(path, new ReaderOptions(conf).maxLength(1))
    val schema = reader.getSchema()
    OrcSchemaFns.fromOrcType(schema).asInstanceOf[StructType]
  }

  private def reader() = {
    val options = new ReaderOptions(conf)
    OrcFile.createReader(path, options)
  }

  def count(): Long = reader().getNumberOfRows
  def statistics(): Seq[ColumnStatistics] = reader().getStatistics.toVector
  def stripes(): Seq[StripeInformation] = reader().getStripes.asScala
  def stripeStatistics(): Seq[StripeStatistics] = reader().getStripeStatistics.asScala
}

class OrcPart(path: Path,
              projection: Seq[String],
              predicate: Option[Predicate])(implicit conf: Configuration) extends Part {

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {
    val reader = OrcFile.createReader(path, new ReaderOptions(conf))
    val fileSchema = OrcSchemaFns.fromOrcType(reader.getSchema).asInstanceOf[StructType]
    override val iterator: Iterator[Seq[Row]] = OrcBatchIterator(reader, fileSchema, projection, predicate)
  }
}

