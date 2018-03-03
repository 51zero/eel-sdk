package io.eels.component.jdbc

import java.sql
import java.sql.DriverManager
import java.util.UUID

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema._
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class JdbcSinkTest extends WordSpec with Matchers with OneInstancePerTest {

  Class.forName("org.h2.Driver")
  private val url = "jdbc:h2:mem:" + UUID.randomUUID.toString.replace("-", "")
  private val conn = DriverManager.getConnection(url)
  conn.createStatement().executeUpdate("create table mytab (a integer, b integer, c integer)")

  private val schema = StructType(Field("a"), Field("b"), Field("c"))
  private val frame = DataStream.fromRows(schema, Row(schema, Vector("1", "2", "3")), Row(schema, Vector("4", "5", "6")))

  "JdbcSink" should {
    "write frame to table" in {
      frame.to(JdbcSink(url, "mytab"))
      val rs = conn.createStatement().executeQuery("select count(*) from mytab")
      rs.next()
      rs.getLong(1) shouldBe 2L
      rs.close()
    }
    "create table if createTable is true" in {
      frame.to(JdbcSink(url, "qwerty").withCreateTable(true))
      val rs = conn.createStatement().executeQuery("select count(*) from qwerty")
      rs.next()
      rs.getLong(1) shouldBe 2L
      rs.close()
    }
    "support multiple writers" in {
      val rows = List.fill(10000)(Row(schema, Vector("1", "2", "3")))
      val ds = DataStream.fromRows(schema, rows)
      val sink = JdbcSink(url, "multithreads").withCreateTable(true).withThreads(4)
      ds.to(sink)
      val rs = conn.createStatement().executeQuery("select count(*) from multithreads")
      rs.next()
      rs.getLong(1) shouldBe 10000L
      rs.close()
    }
    "handle rows larger than batch size" in {
      val rows = List.fill(5000)(Row(schema, Vector("1", "2", "3")))
      val ds = DataStream.fromRows(schema, rows)
      val sink = JdbcSink(url, "batches").withCreateTable(true).withThreads(1).withBatchSize(100)
      ds.to(sink)
      val rs = conn.createStatement().executeQuery("select count(*) from batches")
      rs.next()
      rs.getLong(1) shouldBe 5000
      rs.close()
    }

    "handle row with different array fields" in {

      conn.createStatement().executeUpdate(
        """
          |create table mytab_with_array_columns (
          |  string_array_col array,
          |  int_array_col array,
          |  long_list_col array
          |)""".stripMargin)

      val schemaWithArray = StructType(
        Field("string_array_col", ArrayType(StringType)),
        Field("int_array_col", ArrayType(IntType())),
        Field("long_list_col", ArrayType(LongType()))
      )

      val ds = DataStream.fromRows(schemaWithArray, Seq(
        Row(
          schemaWithArray,
          Vector(
            Vector("foo", "bar"),
            Vector(42, 1234),
            List(42L, 2345L)
          )
        )
      ))
      ds.to(JdbcSink(url, "mytab_with_array_columns"))

      val rs = conn.createStatement().executeQuery(
        """
          |select string_array_col
          |     , int_array_col
          |     , long_list_col
          |  from mytab_with_array_columns""".stripMargin)
      rs.next()
      val sqlStringArray: sql.Array = rs.getArray("string_array_col")
      val javaStringArray: Array[AnyRef] = sqlStringArray.getArray().asInstanceOf[Array[AnyRef]]
      val scalaStringArray = javaStringArray.map(_.asInstanceOf[String])
      scalaStringArray shouldBe Array("foo", "bar")

      val sqlIntArray: sql.Array = rs.getArray("int_array_col")
      val javaIntArray: Array[AnyRef] = sqlIntArray.getArray().asInstanceOf[Array[AnyRef]]
      val scalaIntArray = javaIntArray.map(_.asInstanceOf[Int])
      scalaIntArray shouldBe Array(42, 1234)

      val sqlLongArray: sql.Array = rs.getArray("long_list_col")
      val javaLongArray: Array[AnyRef] = sqlLongArray.getArray().asInstanceOf[Array[AnyRef]]
      val scalaLongArray = javaLongArray.map(_.asInstanceOf[Long])
      scalaLongArray shouldBe Array(42L, 2345L)

      rs.close()

    }
  }
}
