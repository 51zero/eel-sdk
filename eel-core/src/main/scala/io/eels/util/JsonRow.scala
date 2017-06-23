package io.eels.util

import com.fasterxml.jackson.databind.node.ObjectNode
import io.eels.Row

object JsonRow {
  def apply(row: Row): ObjectNode = {
    val node = JacksonSupport.mapper.createObjectNode()
    row.map.foreach {
      case (name, str: String) => node.put(name, str)
      case (name, long: Long) => node.put(name, long)
      case (name, int: Integer) => node.put(name, int)
      case (name, bool: Boolean) => node.put(name, bool)
      case (name, double: Double) => node.put(name, double)
      case (name, float: Float) => node.put(name, float)
      case (name, str) => node.put(name, str.toString)
    }
    node
  }
}
