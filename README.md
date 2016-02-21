# Eel

[![Build Status](https://travis-ci.org/eel-sdk/eel.svg?branch=master)](https://travis-ci.org/eel-sdk/eel)

![Logo](eel-core/src/main/graphics/eel_small.png)

Eel is a toolkit for manipulating data in the hadoop ecosystem. In contrast to distributed batch or streaming engines such as [Spark](http://spark.apache.org/) or [Flink](https://flink.apache.org/), Eel operates on a single machine. It is aimed at those problems which can be solved simply and quickly by iterating over data row by row. Or when the size of the data fits into memory and shuffling data between nodes adds overhead and complexity. This turns out to be quite a common use case.

Latest release [here](http://search.maven.org/#search|ga|1|io-eels)

*Rationale:*

I was working on a big data project - kafka, hdfs, hive, spark. And spark was a great toolkit. But sometimes we wanted to work on some tables, or some files, that were quite small. For instance a feed table that sits alongside a data table. Maybe the feed table had 1000 rows. For this kind of manipulation, submitting a job to Yarn seemed overkill (and would take an order of magnitude longer). Of course, we can use spark local mode, but this doesn't feel like the right way to use Spark.

Eel is a small toolkit for moving data between file formats and databases, when that data will comfortably fit into memory. No distributed programming here! Eel wraps the client libraries available for the many "big data" formats/stores, and uses those to convert into an intermediate format called a 'Frame'. Frames can be mapped, filtered, joined, etc, then persisted back out.

### Example Use Cases

* A great use case is merging many parquet files into a single file. This functionality is not hard to write. You could make a parquet reader, a parquet writer, read -> write loop, close. But its 25 lines of code, when it should be as simple as "from" -> "to". Eel does this.

* Reading from a kafka queue directly into parquet/hive/etc

* Coalescing spark output

* Dumping data from JDBC into Hadoop

### Introduction

The core data structure in Eel is the Frame. A frame consists of columns, and rows containings values for each column. A frame is conceptually similar to a table in a relational database, or a dataframe in Spark, or a dataset in Flink. Frames are constructed from sources such as hive tables, jdbc databases, delimited files, kafka queues, or even programatically from Scala or Java collections.

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

### Components

* CSV
* Json
* JDBC
* Parquet
* Avro
* Hadoop Sequence files
* Hadoop Orc
* Kafka
* Elasticsearch
* Solr

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
hive:<db>:<table>(/col1,col2,col3..,coln)(?options)

Examples:
hive:prod:customers
hive:prod:orders/orderid,date,customerid
hive:prod:accounts?threads=4

##### Csv Source Url

In general:
csv:<path>(/col1,col2,col3..,coln)(?options)

Examples:
csv:/some/path
csv:/some/path:orderid,customerid
csv:/some/path?a=b

### Changelog

* 0.27.0 21/02/15 
 - jdbc auto commit and warn on re-write options
 - fix for parquet projections when no columns specified (default to all)
 - added dataset url parsers
 - removed plans from external api and updated actions to accept scala execution context
* 0.26.0 18/02/15 - Projection pushdowns in Parquet
* 0.25.0 17/02/15 - Removed lots of boxing artifacts
* 0.23.0 16/02/15 - Added mongo sink. Added replace, fill, dropNull and explode methods to frame. Added Avro serializer/deserializer to kafka components.
* 0.22.0 15/02/15 - Kafka sink and kafka source added
* 0.21.0 14/02/15 - Dynamic partioning added on write to Hive sink
* 0.20.1 13/02/15 - Contention issues fixed in hive sink
* 0.20.0 12/02/15 - Orc and Avro table support added to Hive components
* 0.19.0 11/02/15 - Parquet table support added to Hive and Hive now supports writing
* 0.18.0 10/02/15 - Multi threaded read/writes now supported

### How to use

Eel is released to maven central, so is very easy to include in your project. Just find the latest version on [maven central](http://search.maven.org/#search|ga|1|io.eels) and copy the includes.
