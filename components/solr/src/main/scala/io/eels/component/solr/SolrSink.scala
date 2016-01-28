package io.eels.component.solr

import io.eels.{Row, Sink, Writer}
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument

class SolrSink(url: String) extends Sink {
  override def writer: Writer = new Writer {

    val url = "http://localhost:8983/solr"
    val client = new HttpSolrClient(url)
    client.setSoTimeout(1000)

    override def close(): Unit = client.close()

    override def write(row: Row): Unit = {
      val doc = new SolrInputDocument()
      row.toMap.foreach { case (key, value) =>
        doc.addField(key, value)
      }
      client.add(doc)
      client.commit()
    }
  }
}
