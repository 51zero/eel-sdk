package io.eels.component.hbase

import javax.sql.DataSource

import com.sksamuel.exts.Logging
import io.eels.component.jdbc.JdbcSource
import io.eels.schema._
import org.apache.commons.dbcp.BasicDataSource
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}

object HbaseTestApp extends App with HbaseTests with Logging {
  // Setup JDBC data in H2 in-memory database
  private val dataSource = new BasicDataSource
  dataSource.setDriverClassName("org.h2.Driver")
  dataSource.setUrl("jdbc:h2:mem:hbase_testing")
  dataSource.setInitialSize(4)
  createSourceData()

  private val cluster = startHBaseCluster("hbase-cluster")
  createHBaseTables()

  testHbase()

  System.exit(0)

  def testHbase(): Unit = {
    val hbaseConnection = ConnectionFactory.createConnection(cluster.getConfiguration)

    val hbaseSchema = StructType(
      Field(name = "NAME", dataType = StringType, key = true),
      Field(name = "AGE", dataType = IntType.Signed, columnFamily = Option("cf1")),
      Field(name = "SALARY", dataType = DecimalType(38, 5), columnFamily = Option("cf1"))
    )

    val query = "SELECT NAME, AGE, SALARY FROM PERSON"

    logger.info("Writing from JDBC to HBase...")
    JdbcSource(() => dataSource.getConnection, query)
      .toDataStream()
      .to(HbaseSink(namespace = "test", table = "person", connection = hbaseConnection)
        // .withSerializer(HbaseSerializer.orderedDescendingSerializer)
        .withSchema(hbaseSchema)
      )

    logger.info("Reading from HBase...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      // .withSerializer(HbaseSerializer.orderedDescendingSerializer)
      .withSchema(hbaseSchema)
      .toDataStream()
      .collect
      .foreach(println)

    logger.info("Reading a specific row using the key...")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      // .withSerializer(HbaseSerializer.orderedDescendingSerializer)
      .withSchema(hbaseSchema)
      .withPredicate(HbasePredicate.equals("NAME", "Gary"))
      .toDataStream()
      .collect
      .foreach(println)

    logger.info("Reading rows with projection")
    HbaseSource(namespace = "test", table = "person", connection = hbaseConnection)
      // .withSerializer(HbaseSerializer.orderedDescendingSerializer)
      .withSchema(hbaseSchema)
      .withProjection("AGE")
      .toDataStream()
      .collect
      .foreach(println)


    hbaseConnection.close()
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
