package pellucid
package pframe

import scala.collection.immutable.BitSet

import shapeless._
import shapeless.syntax.typeable._

trait Column[A] {
  def exists(row: Int): Boolean
  def missing(row: Int): Missing
  def value(row: Int): A

  def apply(row: Int): Cell[A] =
    if (exists(row)) Value(value(row)) else missing(row)

  /**
   * Map all existing values to a given value.
   */
  def map[B](f: A => B): Column[B] = new MappedColumn(f, this)

  /**
   * Filter a column by a given predicate. All values that have been filtered
   * out are turned into `NA` (Not Available).
   */
  def filter(f: A => Boolean): Column[A] = new FilteredColumn(f, this)

  /**
   * Masks this column with a given `BitSet`. That is, a value only exists at
   * a row if it exists in both the source `Column` and if `bitset(row)` is
   * `true`. If a value exists in the source `Column`, but `bitset(row)` is
   * `false`, then that value is treated as `NA` (Not Available).
   */
  def mask(bits: Int => Boolean): Column[A] = new MaskedColumn(bits, this)

  /**
   * Shift all rows in this column down by `rows`. If `rows` is negative, then
   * they will be shifted up by `-rows`.
   */
  def shift(rows: Int): Column[A] = new ShiftColumn(rows, this)

  /**
   * This method should be used to return a column specialized on a particular
   * index. That means it can drop all values that aren't being accessed by
   * the index and remove some of the indirection built-up via things like
   * `map`, `filter`, etc.
   */
  def optimize(index: Index[_]): Column[A] = ???

  override def toString: String =
    (0 until 5).map(apply(_)).mkString("Column(", ", ", ")")
}

object Column {
  def apply[A](values: Array[A]): Column[A] = DenseColumn(BitSet.empty, BitSet.empty, values)

  def apply[A](f: Int => A): Column[A] = InfiniteColumn(f)

  def apply[A](values: Map[Int, A]): Column[A] = MapColumn(values)

  def empty[A] = new EmptyColumn[A]
}

final class EmptyColumn[A] extends Column[A] {
  def exists(row: Int): Boolean = false
  def missing(row: Int): Missing = NA
  def value(row: Int): A = throw new UnsupportedOperationException()
}

final class ShiftColumn[A](shift: Int, underlying: Column[A]) extends Column[A] {
  def exists(row: Int): Boolean = underlying.exists(row - shift)
  def missing(row: Int): Missing = underlying.missing(row - shift)
  def value(row: Int): A = underlying.value(row - shift)
}

final class MappedColumn[A, B](f: A => B, underlying: Column[A]) extends Column[B] {
  def exists(row: Int): Boolean = underlying.exists(row)
  def missing(row: Int): Missing = underlying.missing(row)
  def value(row: Int): B = f(underlying.value(row))
}

final class FilteredColumn[A](f: A => Boolean, underlying: Column[A]) extends Column[A] {
  def exists(row: Int): Boolean = underlying.exists(row) && f(underlying.value(row))
  def missing(row: Int): Missing = if (underlying.exists(row)) NA else underlying.missing(row)
  def value(row: Int): A = underlying.value(row)
}

final class MaskedColumn[A](bits: Int => Boolean, underlying: Column[A]) extends Column[A] {
  def exists(row: Int): Boolean = underlying.exists(row) && bits(row)
  def missing(row: Int): Missing = if (!bits(row)) NA else underlying.missing(row) // TODO: Should swap order.
  def value(row: Int): A = underlying.value(row)
}

final class CastColumn[A: Typeable](col: Column[_]) extends Column[A] {
  def exists(row: Int): Boolean = col.exists(row) && col.apply(row).cast[A].isDefined
  def missing(row: Int): Missing = if (!col.exists(row)) col.missing(row) else NM
  def value(row: Int): A = col.value(row).cast[A].get
}

case class InfiniteColumn[A](f: Int => A) extends Column[A] {
  def exists(row: Int): Boolean = true
  def missing(row: Int): Missing = NA
  def value(row: Int): A = f(row)
}

case class DenseColumn[A](naValues: BitSet, nmValues: BitSet, values: Array[A]) extends Column[A] {
  private final def valid(row: Int) = row >= 0 && row < values.length
  def exists(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def missing(row: Int): Missing = if (nmValues(row)) NM else NA
  def value(row: Int): A = values(row)
}

case class CellColumn[A](values: IndexedSeq[Cell[A]]) extends Column[A] {
  def exists(row: Int): Boolean = !values(row).isMissing
  def missing(row: Int): Missing = values(row) match {
    case Value(_) => throw new IllegalStateException()
    case (ms: Missing) => ms
  }
  def value(row: Int): A = values(row) match {
    case Value(x) => x
    case (_: Missing) => throw new IllegalStateException()
  }
}

case class MapColumn[A](values: Map[Int,A]) extends Column[A] {
  def exists(row: Int): Boolean = values contains row
  def missing(row: Int): Missing = NA
  def value(row: Int): A = values(row)
}
