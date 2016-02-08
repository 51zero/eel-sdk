package io.eels

trait Frame {
  outer =>

  def schema: FrameSchema

  private[eels] def parts: Seq[Part]

  /**
    * Note: Reduces parts to 1.
    */
  def join(other: Frame): Frame = new Frame {

    override def schema: FrameSchema = outer.schema.join(other.schema)

    override private[eels] def parts: Seq[Part] = {
      val part = new Part {
        override def iterator: Iterator[Row] = new Iterator[Row] {

          val iter1 = outer.parts.map(_.iterator).reduceLeft((a, b) => a ++ b)
          val iter2 = other.parts.map(_.iterator).reduceLeft((a, b) => a ++ b)

          override def hasNext: Boolean = iter1.hasNext && iter2.hasNext
          override def next(): Row = iter1.next() join iter2.next
        }
      }
      Seq(part)
    }
  }

  /**
    * Returns a new Frame where only each "step" row is retained. Ie, if step is 2 then rows 1,3,5,7 will be
    * retainined and if step was 10, then 1,11,21,31 etc.
    */
  def step(k: Int): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts.map { part =>
      Part(
        () => part.iterator.grouped(k).withPartial(true).map(_.head)
      )
    }
  }

  def addColumn(name: String, defaultValue: String): Frame = new Frame {
    override val schema: FrameSchema = outer.schema.addColumn(name)
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.addColumn(name, defaultValue))
  }

  def removeColumn(name: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.removeColumn(name)
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.removeColumn(name))
  }

  def ++(frame: Frame): Frame = union(frame)
  def union(other: Frame): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts ++ other.parts
  }

  def projection(first: String, rest: String*): Frame = projection(first +: rest)
  def projection(columns: Seq[String]): Frame = new Frame {
    override val schema: FrameSchema = FrameSchema(columns.map(Column.apply).toList)
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.projection(columns))
  }

  /**
    * Execute a side effect function for every row in the frame, returning the same Frame.
    *
    * @param f the function to execute
    * @return this frame, to allow for builder style chaining
    */
  def foreach[U](f: (Row) => U): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.foreach(f))
  }

  def collect(pf: PartialFunction[Row, Row]): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.collect(pf))
  }

  /**
    * Note: Reduces parts to 1.
    */
  def drop(k: Int): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = {
      val part = new Part {
        override def iterator: Iterator[Row] = {
          val iters = outer.parts.map(_.iterator).reduceLeft((a, b) => a ++ b)
          iters.drop(k)
        }
      }
      Seq(part)
    }
  }

  def map(f: Row => Row): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.map(f))
  }

  def filterNot(p: Row => Boolean): Frame = filter(str => !p(str))

  def filter(p: Row => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.filter(p))
  }

  /**
    * Filters where the given column matches the given predicate.
    */
  def filter(column: String, p: String => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override private[eels] def parts: Seq[Part] = outer.parts.map(_.filter(column, p))
  }

  // -- actions --
  def size: Plan[Long] = new ToSizePlan(this)

  def toList: Plan[List[Row]] = new ToListPlan(this)

  def forall(p: (Row) => Boolean): Plan[Boolean] = new ForallPlan(this, p)

  def to(sink: Sink): Plan[Int] = new SinkPlan(sink, this)

  def exists(p: (Row) => Boolean): Plan[Boolean] = new ExistsPlan(this, p)

  def find(p: (Row) => Boolean): Plan[Option[Row]] = new FindPlan(this, p)

  def head: Plan[Option[Row]] = new HeadPlan(this)
}

object Frame {

  def apply(first: Row, rest: Row*): Frame = new Frame {
    override lazy val schema: FrameSchema = FrameSchema(first.columns)
    override private[eels] def parts: Seq[Part] = {
      val part = new Part {
        override def iterator: Iterator[Row] = (first +: rest).iterator
      }
      Seq(part)
    }
  }

  def fromSource(source: Source): Frame = new Frame {
    override lazy val schema: FrameSchema = source.schema
    override private[eels] def parts: Seq[Part] = source.parts
  }
}