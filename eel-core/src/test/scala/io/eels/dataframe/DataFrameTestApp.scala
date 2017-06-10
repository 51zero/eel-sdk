package io.eels.dataframe

import java.nio.file.Paths

import io.eels.component.csv.CsvSource

object DataFrameTestApp extends App {

  implicit val em = ExecutionManager.local

  val file = getClass.getResource("/uk-500.csv").toURI()
  val path = Paths.get(file)
  val source = CsvSource(path)
  val ds = source.toDataStream()
  val result = ds.filter(row => row.values.contains("Kent")).collect
  println(result)
}
