package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.schema._
import org.scalatest.{FlatSpec, Matchers}

// tests that the schema mappings for jdbc are correct using h2
class H2SchemaCompatibilityTest extends FlatSpec with Matchers {

  Class.forName("org.h2.Driver")

  val conn = DriverManager.getConnection("jdbc:h2:mem:catalog")
  conn.createStatement().executeUpdate("create table t (" +
    "a integer, " +
    "b bit, " +
    "c bigint, " +
    "d double, " +
    "e time, " +
    "f date, " +
    "g timestamp, " +
    "h decimal(14,5)," +
    "i text," +
    "j smallint," +
    "k tinyint," +
    "l varchar(55)," +
    "m char(15)," +
    "n numeric(66,5)" +
    ")")


  "JdbcSource" should "map schemas from jdbc to correct eel types" in {
    JdbcSource(() => conn, "select * from t").schema shouldBe
      StructType(
        Vector(
          Field("A", IntType(true), true, false),
          Field("B", io.eels.schema.BooleanType, true, false),
          Field("C", io.eels.schema.LongType.Signed, true, false),
          Field("D", io.eels.schema.DoubleType, true, false),
          Field("E", io.eels.schema.TimeMillisType, true, false),
          Field("F", io.eels.schema.DateType, true, false),
          Field("G", io.eels.schema.TimestampMillisType, true, false),
          Field("H", DecimalType(Precision(14), Scale(5)), true, false),
          Field("I", io.eels.schema.StringType, true, false),
          Field("J", io.eels.schema.ShortType(true), true, false),
          Field("K", io.eels.schema.ShortType(true), true, false),
          Field("L", io.eels.schema.VarcharType(55), true, false),
          Field("M", io.eels.schema.CharType(15), true, false),
          Field("N", DecimalType(Precision(66), Scale(5)), true, false)
        )
      )
  }

  "JdbcTable" should "Check primary key info for person table" in {

    implicit val myConn = DriverManager.getConnection("jdbc:h2:mem:catalog")
    val databaseMetaData = myConn.getMetaData
    myConn.createStatement().executeUpdate(
      """
        |CREATE TABLE person(
        |          personID INTEGER AUTO_INCREMENT PRIMARY KEY,
        |          name VARCHAR(100) NOT NULL,
        |          entryDate DATE,
        |          lastUpdated TIMESTAMP)
      """.stripMargin)

    // With catalog, schema and table
    JdbcTable(tableName = "person", catalog = Option("CATALOG"), dbSchema = Option("PUBLIC"))
      .schema
      .fields
      .find(_.name.toUpperCase == "PERSONID").get.key.shouldBe(true)

    // With schema and table
    JdbcTable(tableName = "person", dbSchema = Option("PUBLIC"))
      .schema
      .fields
      .find(_.name.toUpperCase == "PERSONID").get.key.shouldBe(true)

    // With table only
    JdbcTable(tableName = "person")
      .schema
      .fields
      .find(_.name.toUpperCase == "PERSONID").get.key.shouldBe(true)

    // Test primarykey method only
    JdbcTable(tableName = "person").primaryKeys shouldBe Seq("PERSONID")
  }

  "JdbcTable for View" should "No primary keys returned for views" in {

    implicit val myConn = DriverManager.getConnection("jdbc:h2:mem:catalog2")
    val databaseMetaData = myConn.getMetaData
    myConn.createStatement().executeUpdate(
      """
        |CREATE TABLE person(
        |          personID INTEGER AUTO_INCREMENT PRIMARY KEY,
        |          name VARCHAR(100) NOT NULL,
        |          entryDate DATE,
        |          lastUpdated TIMESTAMP)
      """.stripMargin)

    myConn.createStatement().executeUpdate(
      """
        |CREATE VIEW v_person
        |AS
        |SELECT * FROM person
      """.stripMargin)

    // With catalog, schema and table
    JdbcTable(tableName = "v_person", catalog = Option("CATALOG2"), dbSchema = Option("PUBLIC"))
      .schema
      .fields
      .find(_.name.toUpperCase == "PERSONID").get.key.shouldBe(false)

    // With schema and table
    JdbcTable(tableName = "v_person", dbSchema = Option("PUBLIC"))
      .schema
      .fields
      .find(_.name.toUpperCase == "PERSONID").get.key.shouldBe(false)

    // With table only
    JdbcTable(tableName = "v_person")
      .schema
      .fields
      .find(_.name.toUpperCase == "PERSONID").get.key.shouldBe(false)

    // Test primarykey method only - should be empty
    JdbcTable(tableName = "v_person").primaryKeys shouldBe Seq.empty
  }

}