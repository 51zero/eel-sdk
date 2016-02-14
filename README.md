# Eel

[![Build Status](https://travis-ci.org/eel-sdk/eel.svg?branch=master)](https://travis-ci.org/eel-sdk/eel)

![Logo](core/src/main/graphics/eel.png)

Toolkit for using big data technologies in the small.

Latest release [here](http://search.maven.org/#search|ga|1|io-eels)

*Rationale:*

I was working on a big data project - kafka, hdfs, hive, spark. And spark was a great toolkit. But sometimes we wanted to work on some tables, or some files, that were quite small. For instance a feed table that sits alongside a data table. Maybe the feed table had 1000 rows. For this kind of manipulation, submitting a job to Yarn seemed overkill (and would take an order of magnitude longer). Of course, we can use spark local mode, but this doesn't feel like the right way to use Spark.

Eel is a small toolkit for moving data between file formats and databases, when that data will comfortably fit into memory. No distributed programming here! Eel wraps the client libraries available for the many "big data" formats/stores, and uses those to convert into an intermediate format called a 'Frame'. Frames can be mapped, filtered, joined, etc, then persisted back out.

### Example Use Cases

* A great use case is merging many parquet files into a single file. This functionality is not hard to write. You could make a parquet reader, a parquet writer, read -> write loop, close. But its 25 lines of code, when it should be as simple as "from" -> "to". Eel does this.

* Reading from a kafka queue directly into parquet/hive/etc

* Coalescing spark output

* Dumping data from JDBC into Hadoop

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

### How to use

Eel is released to maven central, so is very easy to include in your project. Just find the latest version on [maven central](http://search.maven.org/#search|ga|1|io.eels) and copy the includes.
