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

object HbaseTestApp extends App with HbaseTests with Logging {

  private val serializer: HbaseSerializer = HbaseSerializer.standardSerializer

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
  createHBaseTables()

  val hbaseConnection = ConnectionFactory.createConnection(cluster.getConfiguration)

  // readWriteWithSchema()
  // readWriteWithFieldDefs()
  // readingWithSpecificRow()
  // readingHbaseSourceStats()
  // readWithRowKeyPrefix()
  // readWithKeyRange()
  readWithNumericRange()


  hbaseConnection.close()
  System.exit(0)

  def readWriteWithSchema(): Unit = {
    upsertData()

    println("Reading from HBase...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .toDataStream()
      .collect
      .foreach(println)
  }

  def readWriteWithFieldDefs(): Unit = {
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


  def readingWithSpecificRow(): Unit = {
    upsertData()

    println("Reading a specific row using the key 'Gary'...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(HbasePredicate.equals("NAME", "Gary"))
      .toDataStream()
      .collect
      .foreach(println)

    println("Reading rows with projection")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withProjection("AGE")
      .toDataStream()
      .collect
      .foreach(println)
  }

  def readWithRowKeyPrefix(): Unit = {
    upsertData()

    println("Reading specific rows using row key prefix of 'G' ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withRowPrefixFilter("G")
      .toDataStream()
      .collect
      .foreach(println)
  }

  def readWithKeyRange(): Unit = {
    upsertData()

    println("Reading specific rows with filter: Key >= 'F' and Key <= 'G' ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withKeyValueRange(startKey = "F", stopKey = "G", stopKeyInclusive = true)
      .toDataStream()
      .collect
      .foreach(println)

    println("Reading specific rows with filter: Key >= 'F' and Key < 'G' ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withKeyValueRange(startKey = "F", stopKey = "G")
      .toDataStream()
      .collect
      .foreach(println)

    println("Reading specific rows with filter: Key >= 'F' ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withKeyValue(startKey = "F")
      .toDataStream()
      .collect
      .foreach(println)
  }

  def readWithNumericRange(): Unit = {
    upsertData()

    println("Reading specific rows with filter: age >= 50 ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.gte("AGE", 50))
      .toDataStream()
      .collect
      .foreach(println)

    println("Reading specific rows with filter: age >= 30 and age <= 50 ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.and(Predicate.gte("AGE", 30), Predicate.lte("AGE", 50)))
      .toDataStream()
      .collect
      .foreach(println)

    println("Reading specific rows with filter: age = 33 or age = 46 ...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .withSerializer(serializer)
      .withSchema(hbaseSchema)
      .withPredicate(Predicate.or(Predicate.equals("AGE", 33), Predicate.equals("AGE", 46)))
      .toDataStream()
      .collect
      .foreach(println)

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

  def readingHbaseSourceStats(): Unit = {
    upsertData()

    println("Reading stats...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      .statistics()
      .columnFamilies
      .foreach(println)
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
