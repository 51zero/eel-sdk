# Eel

[![Build Status](https://travis-ci.org/sksamuel/eel-sdk.svg?branch=master)](https://travis-ci.org/sksamuel/eel-sdk)
[<img src="https://img.shields.io/maven-central/v/io.eels/eel-core_2.11*.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22eel-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/io.eels/eel-core_2.12*.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22eel-core_2.12%22)

Eel is a toolkit for manipulating data in the hadoop ecosystem. In contrast to distributed batch or streaming engines such as [Spark](http://spark.apache.org/) or [Flink](https://flink.apache.org/), Eel is an SDK.
![eel logo](https://raw.githubusercontent.com/eel-sdk/eel/master/eel-core/src/main/graphics/eel_small.png)

### Example Use Cases

* A great use case is merging many parquet files into a single file. This functionality is not hard to write. You could make a parquet reader, a parquet writer, read -> write loop, close. But its 25 lines of code, when it should be as simple as "from" -> "to". Eel does this.

* Reading from a kafka queue directly into parquet/hive/etc

* Coalescing spark output

* Dumping data from JDBC into Hadoop

### Overview

The core data structure in Eel is the Frame. A frame consists of columns, and rows containings values for each column. A frame is conceptually similar to a table in a relational database, or a dataframe in Spark, or a dataset in Flink. Frames are constructed from sources such as hive tables, jdbc databases, delimited files, kafka queues, or even programatically from Scala or Java collections.

### Frames

### Sources

### Schema

#### Types Supported

|Eel Datatype|JVM Types|
|-----|-------|
|Short|Short|
|Int|Int|
|Double|Double|
|Float|Float|
|Decimal|BigDecimal|
|String|String|
|Binary|Array of Bytes|
|DateTime|java.sql.Date|
|Timestamp|java.sql.Timestamp|


### File API

### Examples

```scala
// copying from avro files to jdbc
AvroSource("test.avro").to(JdbcSink("jdbc:....", "totable", JdbcSinkProps(createTable = true)))
```

```scala
// copying from one jdbc source to another
JdbcSource("jdbc:....", "fromtable").to(JdbcSink("jdbc:....", "totable", JdbcSinkProps(createTable = true)))
```

### Frame Operations

##### Union

##### Join

##### Projection

### Sources

* CSV
* Json
* JDBC
* Parquet
* Avro
* Hadoop Sequence files
* Hadoop Orc

Parquet Source
--------------
The parquet source will read from one or more parquet files. To use the source, create an instance of `ParquetSource` specifying a file pattern or `Path` object. Currently the Parquet source will assume the format of the parquet file is Avro.

Example reading from a single file `ParquetSource(new Path("hdfs:///myfile"))`
Example reading from a wildcard pattern `ParquetSource("hdfs:///user/sam/*"))`

Hive Source
---
The [Hive](https://hive.apache.org/) source will read from a hive table. To use this source, create an instance of `HiveSource` specifying the database name, the table name, any partitions to limit the read. The source also requires instances of the Hadoop [Filesystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html) object, and a [HiveConf](https://hive.apache.org/javadocs/r0.13.1/api/common/org/apache/hadoop/hive/conf/HiveConf.html) object.

Reading all rows from a table is the simplest use case: `HiveSource("mydb", "mytable")`. We can also read rows from a table for a particular partition. For example, to read all rows which have the value '1975' for the partition column 'year': `HiveSource("mydb", "mytable").withPartition("year", "1975")`

The partition clause accepts an operator to perform more complicated querying, such as less than, greater than etc. For example to read all rows which have a *year* less than *1975* we can do: `HiveSource("mydb", "mytable").withPartition("year", "<", "1975")`.


Hive Sink
----
The [Hive](https://hive.apache.org/) sink writes data to Hive tables stored in any of the following formats: ORC (Optimized Row Columnar), Parquet, Avro, or Text delimited.

To configure a Hive Sink, you specify the Hive database, the table to write to, and the format to write in. The sink also requires instances of the Hadoop [Filesystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html) object, and a [HiveConf](https://hive.apache.org/javadocs/r0.13.1/api/common/org/apache/hadoop/hive/conf/HiveConf.html) object.

**Properties**

|Parameter|Description|
|----------|------------------|
|IO Threads|The number of concurrent writes to the sink|
|Dynamic Partitioning|If set to true then any values on partitioned fields that are new, will automatically be created as partitions in the metastore. If set to false, then a new value will throw an error.

**Example**

Simple example of writing to a Hive database `frame.to(HiveSink("mydb", "mytable"))`

We can specify the number of concurrent writes, by using the ioThreads parameter `frame.to(HiveSink("mydb", "mytable").withIOThreads(4))`
 
Csv Source
----

If the schema you need is in the form of the CSV headers, then we can easily parse those to create the schema. But obviously CSV won't encode any type information. Therefore, we can specify an instance of a `SchemaInferrer` which can be customized with rules to determine the correct schema type for each header. So for example, you might say that "name" is a SchemaType.String, or that anything matching "*_id" is a SchemaType.Long. You can also specify the nullability, scale, precision and unsigned. A quick example:

```scala
val inferrer = SchemaInferrer(SchemaType.String, SchemaRule("qty", SchemaType.Int, false), SchemaRule(".*_id", SchemaType.Int))
CsvSource("myfile").withSchemaInferrer(inferrer)
```

Kafka Source
---

Eel integrates with [Kafka](http://kafka.apache.org/) to read messages from a topic or topics into a Frame. To connect to a Kafka server we need an instance of `KafkaSource` with an instance of `KafkaSourceConfig`. The config requires the broker list (host:port,host:port,..), the consumer group to connect as, as well as the topic or topics to read from. In addition we must specify a `KafkaDeserializer` that can convert the incoming byte array from Kafka into an eel Row. 

To create a KafkaSource that uses the default Json message deserialier, we can do: 
`KafkaSource(KafkaSourceConfig("localhost:12345", "consumer"), Set("topic1"), JsonKafkaDeserializer)`

### Command Line Interface

Eel comes with a handy cli interace.

Supports

--source <sourceurl> --sink <sinkurl>

##### Hive Source Url

In general:
hive:<db>:<table>(:col1,col2,col3..,coln)(?options)

Examples:
hive:prod:customers
hive:prod:orders:orderid,date,customerid
hive:prod:accounts?threads=4

##### Csv Source Url

In general:
csv:<path>(/col1,col2,col3..,coln)(?options)

Examples:
csv:/some/path
csv:/some/path:orderid,customerid
csv:/some/path?a=b

### How to use

Eel is released to maven central, so is very easy to include in your project. Just find the latest version on [maven central](http://search.maven.org/#search|ga|1|io.eels) and copy the includes.
