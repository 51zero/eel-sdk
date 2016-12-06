package io.eels.component.orc

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile.ReaderOptions
import org.apache.orc.{ColumnStatistics, OrcFile, StripeInformation, StripeStatistics}
import reactor.core.publisher.Flux

import scala.collection.JavaConverters._

case class OrcSource(path: Path)(implicit conf: Configuration) extends Source with Using {

  override def parts(): List[Part] = List(new OrcPart(path))

  override def schema(): StructType = OrcFns.readSchema(path)

  private def reader() = OrcFile.createReader(path, new ReaderOptions(conf))

  def count(): Long = reader().getNumberOfRows
  def statistics(): Seq[ColumnStatistics] = reader().getStatistics.toVector
  def stripes(): Seq[StripeInformation] = reader().getStripes.asScala
  def stripeStatistics(): Seq[StripeStatistics] = reader().getStripeStatistics.asScala

  class OrcPart(path: Path) extends Part {
    override def data(): Flux[Row] = new OrcReader(path).rows()
  }
}