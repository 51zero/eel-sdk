package io.eels.component.kudu

import com.sksamuel.exts.Logging
import io.eels.component.FlowableIterator
import io.eels.schema._
import io.eels.{Part, Row, Source}
import io.reactivex.Flowable
import org.apache.kudu.client.{KuduClient, RowResultIterator}

import scala.collection.JavaConverters._

case class KuduSource(tableName: String)(implicit client: KuduClient) extends Source with Logging {

  override lazy val schema: StructType = {
    val schema = client.openTable(tableName).getSchema
    KuduSchemaFns.fromKuduSchema(schema)
  }

  override def parts(): Seq[Part] = Seq(new KuduPart(tableName))

  class KuduPart(tableName: String) extends Part {

    override def open(): Flowable[Row] = {

      val projectColumns = schema.fieldNames()
      val table = client.openTable(tableName)

      val scanner = client.newScannerBuilder(table)
        .setProjectedColumnNames(projectColumns.asJava)
        .build()

      val iterator = new Iterator[Row] {
        var iter: Iterator[Row] = Iterator.empty
        override def hasNext: Boolean = iter.hasNext || {
          if (scanner.hasMoreRows) {
            iter = ResultsIterator(schema, scanner.nextRows)
            iter.hasNext
          } else {
            false
          }
        }
        override def next(): Row = iter.next()
      }

      FlowableIterator(iterator, () => scanner.close)
    }
  }
}

object ResultsIterator {
  def apply(schema: StructType, iter: RowResultIterator) = new Iterator[Row] {

    val zipped = schema.fields.zipWithIndex

    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val next = iter.next()
      val values = zipped.map { case (field, index) =>
        field.dataType match {
          case BinaryType => BinaryValueReader.read(next, index)
          case BooleanType => BooleanValueReader.read(next, index)
          case _: ByteType => ByteValueReader.read(next, index)
          case DoubleType => DoubleValueReader.read(next, index)
          case FloatType => FloatValueReader.read(next, index)
          case _: IntType => IntValueReader.read(next, index)
          case _: LongType => LongValueReader.read(next, index)
          case _: ShortType => ShortValueReader.read(next, index)
          case StringType => StringValueReader.read(next, index)
        }
      }
      Row(schema, values)
    }
  }
}

object KuduSource {
  def apply(master: String, table: String): KuduSource = {
    implicit val client = new KuduClient.KuduClientBuilder(master).build()
    KuduSource(table)
  }
}
