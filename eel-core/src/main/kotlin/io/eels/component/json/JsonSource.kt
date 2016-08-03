package io.eels.component.json

import io.eels.Row
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import io.eels.schema.Field
import io.eels.schema.Schema
import io.eels.util.map
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.ObjectReader
import rx.Observable

data class JsonSource(val path: Path) : Source, Using {

  private val reader: ObjectReader = ObjectMapper().reader(JsonNode::class.java)

  private fun createInputStream(path: Path): FSDataInputStream {
    val fs = FileSystem.get(Configuration())
    return fs.open(path)
  }

  override fun schema(): Schema = using(createInputStream(path)) {
    val roots = reader.readValues<JsonNode>(it)
    val node = roots.asSequence().first()
    val fields = node.fieldNames.asSequence().map { Field(it) }.toList()
    Schema(fields)
  }

  override fun parts(): List<Part> = listOf(JsonPart(path))

  inner class JsonPart(val path: Path) : Part {

    val schema = schema()

    fun nodeToRow(node: JsonNode): Row {
      val values = node.elements.map { it.textValue }.asSequence().toList()
      return Row(schema, values)
    }

    override fun data(): Observable<Row> = Observable.create<Row> { sub ->
      val input = createInputStream(path)
      try {
        sub.onStart()
        reader.readValues<JsonNode>(input).asSequence().forEach {
          val row = nodeToRow(it)
          sub.onNext(row)
        }
      } catch (t: Throwable) {
        sub.onError(t)
      }
      sub.onCompleted()
    }
  }
}


