package framian.column

import framian.Cell

import scala.reflect.ClassTag

trait ColumnBuilder[@specialized(Int,Long,Double) A] {
  def addValue(a: A): this.type
  def addNA(): this.type
  def addNM(): this.type
  def add(cell: Cell[A]): this.type
  def result(): Column[A]
}

object ColumnBuilder {
  final def apply[A]()(implicit gen: GenColumnBuilder[A]): ColumnBuilder[A] = gen()
}

trait GenColumnBuilder[A] {
  def apply(): ColumnBuilder[A]
}

trait GenColumnBuilderLow {
  implicit def anyColumnBuilder[A]: GenColumnBuilder[A] =
    new GenColumnBuilder[A] {
      def apply() = new AnyColumnBuilder[A]
    }
}

object GenColumnBuilder extends GenColumnBuilderLow {
  implicit def genericColumnBuilder[A: ClassTag]: GenColumnBuilder[A] =
    new GenColumnBuilder[A] {
      def apply() = new GenericColumnBuilder[A]
    }

  implicit def intColumnBuilder: GenColumnBuilder[Int] =
    new GenColumnBuilder[Int] {
      def apply() = new IntColumnBuilder
    }

  implicit def longColumnBuilder: GenColumnBuilder[Long] =
    new GenColumnBuilder[Long] {
      def apply() = new LongColumnBuilder
    }

  implicit def doubleColumnBuilder: GenColumnBuilder[Double] =
    new GenColumnBuilder[Double] {
      def apply() = new DoubleColumnBuilder
    }
}

// Note: See project/Boilerplate.scala for ColumnBuilder implementations.
