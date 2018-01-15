package io.eels.component.hbase

import javax.sql.DataSource

import com.sksamuel.exts.Logging
import io.eels.Predicate
import io.eels.component.jdbc.JdbcSource
import io.eels.schema._
import org.apache.commons.dbcp.BasicDataSource
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Try

class HbaseFullTests extends FunSuite with HbaseTests with BeforeAndAfterAll with Logging {

  private val serializer: HbaseSerializer = HbaseSerializer.standardSerializer

  val sqlCmds = Seq(
    "CREATE TABLE IF NOT EXISTS PERSON(NAME VARCHAR(30), AGE INT, SALARY NUMBER(38,5))",
    "INSERT INTO PERSON VALUES ('Fred', 55, 650000.34)",
    "INSERT INTO PERSON VALUES ('Gary', 46, 1200000.45)",
    "INSERT INTO PERSON VALUES ('Jane', 21, 350000.23)",
    "INSERT INTO PERSON VALUES ('Steve', 55, 1000000.654)",
    "INSERT INTO PERSON VALUES ('Geoff', 45, 950000.876)",
    "INSERT INTO PERSON VALUES ('Neil', 55, 1050000.23)",
    "INSERT INTO PERSON VALUES ('Alice', 33, 450000.98)",
    "INSERT INTO PERSON VALUES ('May', 29, 650000.34)",
    "INSERT INTO PERSON VALUES ('Joanna', 38, 570000.34)",
    "INSERT INTO PERSON VALUES ('Anne', 58, 880000.34)"
  )

  // Setup JDBC data in H2 in-memory database
  private val dataSource = new BasicDataSource
  dataSource.setDriverClassName("org.h2.Driver")
  dataSource.setUrl("jdbc:h2:mem:hbase_testing")
  dataSource.setInitialSize(4)
  createSourceData()

  val hbaseSchema = StructType(
    Field(name = "NAME", dataType = StringType, key = true),
    Field(name = "AGE", dataType = IntType.Signed, columnFamily = Option("cf1")),
    Field(name = "SALARY", dataType = DecimalType(38, 5), columnFamily = Option("cf1"))
  )

  private val cluster = startHBaseCluster("hbase-cluster")

  val hbaseConnection = ConnectionFactory.createConnection(cluster.getConfiguration)

  override def beforeAll(): Unit = Try {
    createHBaseTables()
  }

  override def afterAll(): Unit = Try {
    hbaseConnection.close()
  }

  test("readWriteWithSchema") {
    upsertData()

    println("Reading from HBase...")
    val list = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .toDataStream()
      .collect

    list.foreach(println)
    assert(list.length == sqlCmds.length - 1)
  }

  test("readWriteWithFieldDefs") {
    println("Writing from JDBC to HBase...")
    val query = "SELECT NAME, AGE, SALARY FROM PERSON"
    JdbcSource(() => dataSource.getConnection, query)
      .toDataStream()
      .to(HbaseSink(namespace = "test", table = "person", connection = hbaseConnection)
        .withSerializer(serializer)
        .withFieldKey("NAME", StringType)
        .withField("AGE", IntType.Signed, "cf1")
        .withField("SALARY", DecimalType(38, 5), "cf1")
      )

    println("Reading from HBase...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withFieldKey("NAME", StringType)
      .withField("AGE", IntType.Signed, "cf1")
      .withField("SALARY", DecimalType(38, 5), "cf1")
      .toDataStream()
      .collect
      .foreach(println)
  }


  test("readingWithSpecificRow") {
    upsertData()

    println("Reading a specific row using the key 'Gary'...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(HbasePredicate.equals("NAME", "Gary"))
      .toDataStream()
      .collect
      .foreach { r =>
        println(r)
        assert(r.get("NAME") == "Gary")
      }


    println("Reading rows with projection")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withProjection("AGE")
      .toDataStream()
      .collect
      .foreach { r =>
        println(r)
        assert(r.values.length == 2) // Column length of projection always includes the key column
      }
  }

  test("readWithRowKeyPrefix") {
    upsertData()

    println("Reading specific rows using row key prefix of 'G' ...")
    val list = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withRowPrefixFilter("G")
      .toDataStream()
      .collect

    // print them
    list.foreach(println)

    // Assert the values
    assert(list.length == 2)
    assert {
      list.count(r => r("NAME").toString.startsWith("G")) == list.length
    }
  }

  test("readWithKeyRange") {
    upsertData()

    println("Reading specific rows with filter: Key >= 'F' and Key <= 'G' ...")
    val names = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withKeyValueRange(startKey = "F", stopKey = "G", stopKeyInclusive = true)
      .toDataStream()
      .collect.map(_ ("NAME").toString)

    names.foreach(println)
    assert(names == Seq("Fred", "Gary", "Geoff"))


    println("Reading specific rows with filter: Key >= 'F' and Key < 'G' ...")
    val names2 = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withKeyValueRange(startKey = "F", stopKey = "G")
      .toDataStream()
      .collect.map(_ ("NAME").toString)

    names2.foreach(println)
    assert(names2 == Seq("Fred"))

    /*  TODO
        println("Reading specific rows with filter: Key >= 'F' ...")
        val names3 = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
          .withSerializer(serializer)
          .withSchema(hbaseSchema)
          .withKeyValue(startKey = "F")
          .toDataStream()
          .collect.map(_ ("NAME").toString)

        names3.foreach(println)
        assert(names3 == Seq("Fred", "Gary", "Jane", "Steve", "Geoff", "Neil", "May", "Joanna").sorted)
    */
  }

  test("readWithNumericRange") {
    upsertData()

    println("Reading specific rows with filter: age >= 50 ...")
    val result1 = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.gte("AGE", 50))
      .toDataStream()
      .collect.map(_ ("NAME").toString)
    result1.foreach(println)
    assert(result1 == Seq("Fred", "Steve", "Neil", "Anne").sorted)

    println("Reading specific rows with filter: age >= 30 and age <= 50 ...")
    val result2 = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.and(Predicate.gte("AGE", 30), Predicate.lte("AGE", 50)))
      .toDataStream()
      .collect.map(_ ("NAME").toString)
    result2.foreach(println)
    assert(result2 == Seq("Gary", "Geoff", "Alice", "Joanna").sorted)

    println("Reading specific rows with filter: age = 33 or age = 46 ...")
    val result3 = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.or(Predicate.equals("AGE", 33), Predicate.equals("AGE", 46)))
      .toDataStream()
      .collect.map(_ ("NAME").toString)
    result3.foreach(println)
    assert(result3 == Seq("Gary", "Alice").sorted)

    // todo not returning rows
    println("Reading specific rows with filter: salary >= 1200000 and salary <= 1200000.99 ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.and(Predicate.gte("SALARY", BigDecimal("1200000")), Predicate.lte("SALARY", BigDecimal("1200000.99"))))
      .toDataStream()
      .collect
      .foreach(println)
  }

  test("readingHbaseSourceStats") {
    upsertData()

    println("Reading stats...")
    val cf = HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .statistics()
      .columnFamilies
    cf.foreach(println)
    assert(cf.length == 1)
  }


  private def upsertData(): Unit = {
    println("Writing from JDBC to HBase...")
    val query = "SELECT NAME, AGE, SALARY FROM PERSON"
    JdbcSource(() => dataSource.getConnection, query)
      .toDataStream()
      .to(HbaseSink(namespace = "test", table = "person", connection = hbaseConnection)
        .withSerializer(serializer)
        .withSchema(hbaseSchema)
      )
  }

  private def createHBaseTables(): Unit = {
    val connection = ConnectionFactory.createConnection(cluster.getConfiguration)
    val admin = connection.getAdmin
    admin.createNamespace(NamespaceDescriptor.create("test").build())
    val hTableDescriptor = new HTableDescriptor(TableName.valueOf("test", "person"))
    hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")))
    admin.createTable(hTableDescriptor)
    connection.close()
  }

  private def createSourceData(): Unit = {
    executeBatchSql(dataSource, sqlCmds)
  }

  private def executeBatchSql(dataSource: DataSource, sqlCmds: Seq[String]): Unit = {
    val connection = dataSource.getConnection
    connection.clearWarnings()
    sqlCmds.foreach { ddl =>
      val statement = connection.createStatement()
      statement.execute(ddl)
      statement.close()
    }
    connection.close()
  }
}
