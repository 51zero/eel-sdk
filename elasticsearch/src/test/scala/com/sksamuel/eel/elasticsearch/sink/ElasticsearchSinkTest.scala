package com.sksamuel.eel.elasticsearch.sink

import java.nio.file.Files

import com.sksamuel.eel.{Frame, Column, Row}
import com.sksamuel.elastic4s.{ElasticDsl, ElasticClient}
import org.elasticsearch.common.settings.Settings
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Span, Seconds}
import org.scalatest.{WordSpec, Matchers}

class ElasticsearchSinkTest extends WordSpec with Matchers with Eventually {

  import ElasticDsl._

  val client = ElasticClient
    .local(Settings.builder().put("path.home", Files.createTempDirectory("estest").toFile.getAbsolutePath).build())

  try {
    client.execute {
      delete index "myindex"
    }.await
  } catch {
    case e: Exception =>
  }

  try {
    client.execute {
      create index "myindex"
    }.await
  } catch {
    case e: Exception =>
  }

  "ElasticsearchSink" should {
    "persist each row" in {

      val frame = Frame(
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("clint eastwood", "actor", "carmel")),
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("elton john", "musician", "pinner"))
      )
      frame.to(ElasticsearchSink(() => client, "myindex", "mytype", closeClient = false))
      eventually(Timeout(Span(5, Seconds))) {
        client.execute {
          search in "myindex" / "mytype" query "*"
        }.await.totalHits shouldBe 2
      }
    }
  }
}
