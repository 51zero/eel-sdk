package io.eels.component.mongo

import com.mongodb.{MongoClientURI, MongoClient}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Row, Sink, Writer}
import org.bson.Document

case class MongoSinkConfig(uri: String, db: String, collection: String)

case class MongoSink(config: MongoSinkConfig)
  extends Sink
    with StrictLogging {

  override def writer: Writer = new Writer {

    val client = new MongoClient(new MongoClientURI(config.uri))
    logger.info(s"Created mongo client [${config.uri}]")

    val db = client.getDatabase(config.db)
    logger.info(s"Connected to mongo db $db")

    val coll = db.getCollection(config.collection)
    logger.info(s"Connected to mongo collection $coll")

    override def close(): Unit = {
      logger.info("Shutting down mongo client")
      client.close()
    }

    override def write(row: Row): Unit = {
      val doc = row.toMap.foldLeft(new Document) { case (d, (key, value)) => d.append(key, value) }
      coll.insertOne(doc)
    }
  }
}