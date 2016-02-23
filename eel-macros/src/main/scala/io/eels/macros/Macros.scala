package io.eels.macros

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

object Macros {
  def fields[T <: Product]: Seq[String] = macro fieldsImpl
  def fieldsImpl[T <: Product](c: whitebox.Context): c.Expr[Seq[String]] = {
    import c.universe._
    c.Expr(q"Nil")
  }
}
