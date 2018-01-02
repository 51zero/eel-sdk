package io.eels.component.hbase

import javax.sql.DataSource

import org.apache.commons.dbcp.BasicDataSource
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FunSuite

class HbaseSourceSinkTests extends FunSuite with HbaseTests {

  // Setup JDBC data in H2 in-memory database
  private val dataSource = new BasicDataSource
  dataSource.setDriverClassName("org.h2.Driver")
  dataSource.setUrl("jdbc:h2:mem:hbase_testing")
  dataSource.setInitialSize(4)
  createSourceData()

  private val cluster = startHBaseCluster("hbase-cluster")
  createHBaseTables()

  test("testIt") {
    val query = "SELECT NAME, AGE, SALARY, CAST(CREATION_TIME AS VARCHAR(255)) AS CREATION_TIME FROM PERSON"
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
      "CREATE TABLE IF NOT EXISTS PERSON(NAME VARCHAR(30), AGE INT, SALARY NUMBER(38,5), CREATION_TIME TIMESTAMP)",
      "INSERT INTO PERSON VALUES ('Fred', 55, 650000.34, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Gary', 46, 1200000.45, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Jane', 21, 350000.23, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Steve', 55, 1000000.654, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Geoff', 45, 950000.876, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Neil', 55, 1050000.23, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Alice', 33, 450000.98, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('May', 29, 650000.34, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Joanna', 38, 570000.34, CURRENT_TIMESTAMP())",
      "INSERT INTO PERSON VALUES ('Anne', 58, 880000.34, CURRENT_TIMESTAMP())"
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
