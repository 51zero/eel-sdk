package io.eels.component.kudu

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Row, Source}
import org.apache.kudu.client.{KuduClient, RowResultIterator}

import scala.collection.JavaConverters._

case class KuduSource(tableName: String)(implicit client: KuduClient) extends Source with Logging {

  override lazy val schema: StructType = {
    val schema = client.openTable(tableName).getSchema
    KuduSchemaFns.fromKuduSchema(schema)
  }

  override def parts(): Seq[Part] = Seq(new KuduPart(tableName))

  class KuduPart(tableName: String) extends Part {

    override def iterator(): CloseableIterator[Seq[Row]] = {

      val projectColumns = schema.fieldNames()
      val table = client.openTable(tableName)

      val scanner = client.newScannerBuilder(table)
        .setProjectedColumnNames(projectColumns.asJava)
        .build()

      val _iter = new Iterator[Row] {
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

      new CloseableIterator[Seq[Row]] {
        override val iterator: Iterator[Seq[Row]] = _iter.grouped(100).withPartial(true)
      }
    }
  }
}

object ResultsIterator {
  def apply(schema: StructType, iter: RowResultIterator) = new Iterator[Row] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val next = iter.next()
      val values = schema.fieldNames.map { fieldName =>
        next.getString(fieldName)
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
