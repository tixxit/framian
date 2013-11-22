package pellucid.pframe

import scala.reflect.runtime.universe.{ TypeTag, typeTag }

import spire.algebra._
import spire.syntax.monoid._

import shapeless._
import shapeless.syntax.typeable._

/**
 * An abstraction for heterogeneously typed columns. We work with them by
 * casting to a real, typed column. Values that cannot be cast are treated as
 * `NM` (not meaningful) values.
 */
trait UntypedColumn extends ColumnLike[UntypedColumn] {
  def cast[A: Typeable: TypeTag]: Column[A]
}

object UntypedColumn {
  implicit object monoid extends Monoid[UntypedColumn] {
    def id: UntypedColumn = empty
    def op(lhs: UntypedColumn, rhs: UntypedColumn): UntypedColumn = (lhs, rhs) match {
      case (EmptyUntypedColumn, _) => rhs
      case (_, EmptyUntypedColumn) => lhs
      case _ => MergedUntypedColumn(lhs, rhs)
    }
  }

  final def empty: UntypedColumn = EmptyUntypedColumn
}

final case object EmptyUntypedColumn extends UntypedColumn {
  def cast[B: Typeable: TypeTag]: Column[B] = Column.empty
  def mask(bits: Int => Boolean): UntypedColumn = EmptyUntypedColumn
  def shift(rows: Int): UntypedColumn = EmptyUntypedColumn
  def reindex(index: Array[Int]): UntypedColumn = EmptyUntypedColumn
  def setNA(row: Int): UntypedColumn = EmptyUntypedColumn
}

case class TypedColumn[A](column: Column[A])(implicit val typeTagA: TypeTag[A]) extends UntypedColumn {
  def cast[B: Typeable: TypeTag]: Column[B] = {
    if (typeTagA.tpe <:< typeTag[B].tpe) {
      column.asInstanceOf[Column[B]]
    } else {
      new CastColumn[B](column)
    }
  }

  def mask(bits: Int => Boolean): UntypedColumn = TypedColumn(column.mask(bits))
  def shift(rows: Int): UntypedColumn = TypedColumn(column.shift(rows))
  def reindex(index: Array[Int]): UntypedColumn = TypedColumn(column.reindex(index))
  def setNA(row: Int): UntypedColumn = TypedColumn(column.setNA(row))
}

case class MergedUntypedColumn(left: UntypedColumn, right: UntypedColumn) extends UntypedColumn {
  def cast[B: Typeable: TypeTag]: Column[B] = left.cast[B] |+| right.cast[B]
  def mask(bits: Int => Boolean): UntypedColumn = MergedUntypedColumn(left.mask(bits), right.mask(bits))
  def shift(rows: Int): UntypedColumn = MergedUntypedColumn(left.shift(rows), right.shift(rows))
  def reindex(index: Array[Int]): UntypedColumn = MergedUntypedColumn(left.reindex(index), right.reindex(index))
  def setNA(row: Int): UntypedColumn = MergedUntypedColumn(left.setNA(row), right.setNA(row))
}
