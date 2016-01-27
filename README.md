# Eel

![Logo](core/src/main/graphics/eel.png)

### Examples

```scala
// copying from avro files to jdbc
AvroSource("test.avro").to(JdbcSink("jdbc:....", "totable", JdbcSinkProps(createTable = true)))
```

```scala
// copying from one jdbc source to another
JdbcSource("jdbc:....", "fromtable").to(JdbcSink("jdbc:....", "totable", JdbcSinkProps(createTable = true)))
```

### Components

* CSV
* JDBC
* Parquet
* Avro
* Kafka
* Elasticsearch