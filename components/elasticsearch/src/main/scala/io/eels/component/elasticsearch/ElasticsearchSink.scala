package io.eels.component.elasticsearch

import com.sksamuel.elastic4s.source.Indexable
import com.sksamuel.elastic4s.{ElasticClient, ElasticDsl, ElasticsearchClientUri}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Row, Sink, Writer}

case class ElasticsearchSink(clientFn: () => ElasticClient,
                             indexName: String,
                             typeName: String,
                             closeClient: Boolean)
  extends Sink with StrictLogging {

  override def writer: Writer = new Writer {

    val client = clientFn()
    logger.debug("Created ES client [$client]")

    override def close(): Unit = if (closeClient) client.close() else ()

    override def write(row: Row): Unit = {

      import ElasticDsl._
      implicit val indexable = IndexableImplicits.RowIndexable

      client.execute {
        index into indexName / typeName source row
      }
    }
  }
}

object ElasticsearchSink {
  def apply(uri: String, indexName: String, typeName: String, closeClient: Boolean): ElasticsearchSink = {
    ElasticsearchSink(
      () => ElasticClient.transport(ElasticsearchClientUri(uri)),
      indexName,
      typeName,
      true
    )
  }
}

object IndexableImplicits {

  import org.json4s.native.Serialization.write
  implicit val formats = org.json4s.DefaultFormats

  implicit object RowIndexable extends Indexable[Row] {
    override def json(t: Row): String = write(t.toMap)
  }
}


