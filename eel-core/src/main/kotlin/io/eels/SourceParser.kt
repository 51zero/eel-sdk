package io.eels

interface SourceParser {
  fun parse(url: String): Builder<Source>?

  companion object SourceParser {
    val parsers = mutableListOf<io.eels.SourceParser>()

    init {
      //      parsers.add(HiveSourceParser)
      //      parsers.add(CsvSourceParser)
      //      parsers.add(ParquetSourceParser)
      //      parsers.add(AvroSourceParser)
    }

    fun apply(url: String): Builder<Source>? {
      // parsers.foldLeft(none[Builder[Source]])((a, parser) => a.orElse(parser(url)))
      return null
    }
  }
}

interface Builder<out T> {
  operator fun invoke(): T
}

interface SinkParser {
  fun parse(url: String): Builder<Sink>?

  companion object SinkParser {
    val parsers = mutableListOf<SinkParser>()

    init {
      //      parsers.add(HiveSinkParser)
      //      parsers.add(CsvSinkParser)
      //      parsers.add(ParquetSinkParser)
    }

    fun apply(url: String): Builder<Sink>? {
      // parsers.foldLeft(none[Builder[Sink]])((a, parser) => a.orElse(parser(url)))
      return null
    }
  }
}