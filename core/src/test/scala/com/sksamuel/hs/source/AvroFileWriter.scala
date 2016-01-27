package com.sksamuel.hs.source

import java.io.File

import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema}

object AvroFileWriter extends App {

  case class Person(name: String, job: String, location: String)

  val schema = AvroSchema[Person]
  val out = AvroOutputStream[Person](new File("test.avro"))

  out.write(Person("clint eastwood", "actor", "carmel"))
  out.write(Person("elton john", "musician", "pinner"))
  out.write(Person("issac newton", "scientist", "heaven"))
  out.close()

}
