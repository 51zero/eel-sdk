package io.eels.source

case class Source(uri: String, params: Map[String, String])

object Source {
  def apply(str: String): Source = {
    str.split('?') match {
      case Array(uri) => Source(uri, Map.empty)
      case Array(uri, queryString) =>
        val params = queryString.split('&').map { kv =>
          kv.split('=') match {
            case Array(key) => key -> ""
            case Array(key, value) => key -> value
          }
        }.toMap
        Source(uri, params)
    }
  }
}