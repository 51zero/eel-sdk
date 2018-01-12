package io.eels.component.hive

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicInteger

import io.eels.component.hive.HiveConfig.client
import org.apache.hadoop.hive.metastore.api.Database

object HiveTestUtils {

  val dbNameTestCounter = new AtomicInteger(0)

  def createTestDatabase: String = {
    val dbName = s"hive_test_${dbNameTestCounter.incrementAndGet()}"
    val dbLocation = new File(".", dbName).getAbsolutePath
    client.createDatabase(new Database(dbName, "Test database for EEL Hive Testing", dbLocation, new util.HashMap[String, String]()))
    dbName
  }
}
