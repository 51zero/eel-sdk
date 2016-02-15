# Eel

[![Build Status](https://travis-ci.org/eel-sdk/eel.svg?branch=master)](https://travis-ci.org/eel-sdk/eel)

![Logo](eel-core/src/main/graphics/eel.png)

Eel is a toolkit for using big data technologies in the small. 

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
* Hive
* Kafka
* Elasticsearch
* Solr
* 

### Changelog

* 0.22.0 15/02/15 - Kafka sink and kafka source added
* 0.21.0 14/02/15 - Dynamic partioning added on write to Hive sink
* 0.20.1 13/02/15 - Contention issues fixed in hive sink
* 0.20.0 12/02/15 - Orc and Avro table support added to Hive components
* 0.19.0 11/02/15 - Parquet table support added to Hive and Hive now supports writing
* 0.18.0 10/02/15 - Multi threaded read/writes now supported

### How to use

Eel is released to maven central, so is very easy to include in your project. Just find the latest version on [maven central](http://search.maven.org/#search|ga|1|io.eels) and copy the includes.
