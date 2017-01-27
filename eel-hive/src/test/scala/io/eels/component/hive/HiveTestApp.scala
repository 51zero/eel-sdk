package io.eels.component.hive

import java.sql.Date
import java.util.Properties

import com.sksamuel.exts.metrics.Timed
import io.eels.Predicate
import io.eels.schema.PartitionConstraint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.io.IOConstants
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.serde2.io.{DateWritable, HiveDecimalWritable, TimestampWritable}
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.JobConf
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object HiveTestApp extends App with Timed {

  implicit val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-2.1.0-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  val path = new Path("hive.test")

  val tableProperties = new Properties
  tableProperties.put(IOConstants.COLUMNS, "myBoolean,myTinyInt,mySmallInt,myInt,myBigInt,myFloat,myDouble,myDecimal,myString,myVarchar,myChar,myTimeStamp,myDate")
  tableProperties.put(IOConstants.COLUMNS_TYPES, "boolean,tinyint,smallint,int,bigint,float,double,decimal(10,3),string,varchar(10),char(3),timestamp,date")
  tableProperties.put(ParquetOutputFormat.COMPRESSION, CompressionCodecName.SNAPPY.name)

  // Create the writer (output format can be ascertained from Hive MetaStore and instantiated via reflection)
  val outputFormat = new MapredParquetOutputFormat
  val jobConf = new JobConf()
  val writer = outputFormat.getHiveRecordWriter(jobConf, path, null, true, tableProperties, null)

  // Create the record (SERDE can be ascertained from Hive MetaStore and instantiated via reflection)
  val parquetHiveSerDe = new ParquetHiveSerDe
  parquetHiveSerDe.initialize(conf, tableProperties)
  val timeStampWritable = new TimestampWritable() // timestamp
  timeStampWritable.setTime(System.currentTimeMillis())

  val row = new ArrayWritable(classOf[ArrayWritable])
  val recordArray = Array[Writable](
    new BooleanWritable(true), // boolean
    new ByteWritable(2), // tinyint
    new ShortWritable(25), // smallint
    new IntWritable(123), // int
    new LongWritable(25L), // bigint
    new FloatWritable(99.99F), // float
    new DoubleWritable(999.99D), // double
    new HiveDecimalWritable(HiveDecimal.create("99.232")), // decimal(10,3)
    new Text("MyString"), // string
    new Text("MyVarchar"), // varchar(10)
    new Text("CHR"), // char(3)
    timeStampWritable, // timestamp
    new DateWritable(new Date(System.currentTimeMillis())) // date
  )
  row.set(recordArray)

  // Write the record
  writer.write(parquetHiveSerDe.serialize(row, parquetHiveSerDe.getObjectInspector))

  // Close the writer
  writer.close(true)

  //  val Database = "sam"
  //  val Table = "foo1"


//  val data = Array(
//    Vector("elton", "yellow brick road ", "1972"),
//    Vector("elton", "tumbleweed connection", "1974"),
//    Vector("elton", "empty sky", "1969"),
//    Vector("beatles", "white album", "1969"),
//    Vector("beatles", "tumbleweed connection", "1966"),
//    Vector("pinkfloyd", "the wall", "1979"),
//    Vector("pinkfloyd", "dark side of the moon", "1974"),
//    Vector("pinkfloyd", "emily", "1966")
//  )
//
//  val rows = List.fill(10000)(data(Random.nextInt(data.length)))
//  val frame = Frame.fromValues(Schema("artist", "album", "year"), rows).addField("bibble", "myvalue").addField("timestamp", System.currentTimeMillis)
//  println(frame.schema.show())
//
//  timed("creating table") {
//    new HiveOps(client).createTable(
//      Database,
//      Table,
//      frame.schema,
//      List("artist"),
//      format = HiveFormat.Parquet,
//      overwrite = true
//    )
//  }
//
//  val table = new HiveOps(client).tablePath(Database, Table)
//
//  val sink = HiveSink(Database, Table).withIOThreads(4)
//  timed("writing data") {
//    frame.to(sink)
//    logger.info("Write complete")
//  }

  //  val footers = ParquetSource(s"hdfs:/user/hive/warehouse/$Database.db/$Table/*").footers
  //
  //  import scala.collection.JavaConverters._
  //
  //  val sum = footers.flatMap(_.getParquetMetadata.getBlocks.asScala.map(_.getRowCount)).sum
  //  println(sum)


  //  timed("hive read") {
  //    val source = HiveSource("sam", "albums").toFrame(4).filter("year", _.toString == "1979")
  //    println(source.size)
  //  }
  //
  //  timed("hive read with predicate") {
  //    val source = HiveSource("sam", "albums").withPredicate(PredicateEquals("year", "1979")).toFrame(4)
  //    println(source.size)
  //  }

//  val size = HiveSource(Database, Table).toFrame(4).size()
 // println(size)

//  val k = HiveSource(Database, Table).withProjection("album").toFrame(4).take(3).toList()
  //  println(k)
  //
  //  val m = HiveSource(Database, Table).withProjection("artist").toFrame(4).take(3).toList()
  //  println(m)
  //
  //  val y = HiveSource(Database, Table).withProjection("artist").withPartitionConstraint(PartitionConstraint.equals("artist", "elton")).toFrame().take(10).toList()
  //  println(y)
  //
  //  val x = HiveSource(Database, Table).withProjection("album").withPredicate(Predicate.equals("album", "white album")).toFrame().take(10).toList()
  //  println(x)
  //
  //  val t = HiveSource(Database, Table).withProjection("album").withPartitionConstraint(PartitionConstraint.equals("artist", "elton")).toFrame().take(10).toList()
  //  println(t)
  //
  //  val w = HiveSource(Database, Table).withProjection("artist").withPredicate(Predicate.equals("album", "elton")).toFrame().take(10).toList()
  //  println(w)
  //
  //  val m = HiveSource(Database, Table).withProjection("artist").withPredicate(Predicate.equals("album", "the wall")).toFrame().take(10).toList()
  //  println(m)


  //val partitionNames = client.listPartitionNames("sam", "albums", Short.MaxValue)
  //  println(partitionNames.asScala.toList)

  //  val result = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1975").toSeq
  //  logger.info("Result=" + result)

  //  val years = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1970").withColumns("year", "artist").toSeq
  //  logger.info("years=" + years)
}
