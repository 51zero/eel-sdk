package io.eels.component.csv

import java.nio.file.Paths

import io.eels.Builder
import io.eels.SinkParser
import io.eels.SourceParser

object CsvSourceParser : SourceParser {
  val Regex = "csv:([^?].*?)(\\?.*)?"
  override fun parse(url: String): Builder<CsvSource>? = when (url) {
  //Regex -> CsvSourceBuilder(path, params)
    else -> null
  }
}

class CsvSourceBuilder(val path: String, params: Map<String, List<String>>) : Builder<CsvSource> {
  override fun invoke(): CsvSource = CsvSource(Paths.get(path))
}

object CsvSinkParser : SinkParser {
  val Regex = "csv:([^?].*?)(\\?.*)?"
  override fun parse(url: String): Builder<CsvSink>? = when (url) {
  // Regex -> Some(CsvSinkBuilder(path, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    else -> null
  }
}

class CsvSinkBuilder(val path: String, params: Map<String, List<String>>) : Builder<CsvSink> {
  override fun invoke(): CsvSink = CsvSink(Paths.get(path))
}