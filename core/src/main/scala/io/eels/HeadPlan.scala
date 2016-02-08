package io.eels

import com.sksamuel.scalax.OptionImplicits._

class HeadPlan(frame: Frame) extends Plan[Option[Row]] {

  override def run: Option[Row] = {
    frame.parts.foldLeft(none[Row])((result, part) => result.orElse(part.iterator.take(1).toList.headOption))
  }
}

class ExistsPlan(frame: Frame, p: (Row) => Boolean) extends Plan[Boolean] {

  override def run: Boolean = {
    frame.parts.foldLeft(false)((exists, part) => if (exists) exists else part.iterator.exists(p))
  }
}

class FindPlan(frame: Frame, p: (Row) => Boolean) extends Plan[Option[Row]] {

  override def run: Option[Row] = {
    frame.parts.foldLeft(none[Row])((result, part) => result.orElse(part.iterator.find(p)))
  }
}