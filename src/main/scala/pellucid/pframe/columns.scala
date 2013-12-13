package pellucid.pframe

import language.experimental.macros
import scala.reflect.macros.Context

import scala.collection.immutable.BitSet
import scala.{ specialized => spec }

import spire.algebra._

import shapeless._
import shapeless.syntax.typeable._

final class EmptyColumn[A] extends Column[A] {
  def exists(row: Int): Boolean = false
  def missing(row: Int): Missing = NA
  def value(row: Int): A = throw new UnsupportedOperationException()
}

final class ConstColumn[@spec(Int,Long,Float,Double) A](value: A) extends Column[A] {
  def exists(row: Int): Boolean = true
  def missing(row: Int): Missing = throw new UnsupportedOperationException()
  def value(row: Int): A = value
}

final class SetNAColumn[A](na: Int, underlying: Column[A]) extends Column[A] {
  def exists(row: Int): Boolean = row != na && underlying.exists(row)
  def missing(row: Int): Missing = if (row == na) NA else underlying.missing(row)
  def value(row: Int): A = underlying.value(row)
}

final class ReindexColumn[A](index: Array[Int], underlying: Column[A]) extends Column[A] {
  private final def valid(row: Int) = row >= 0 && row < index.length
  def exists(row: Int): Boolean = valid(row) && underlying.exists(index(row))
  def missing(row: Int): Missing = if (valid(row)) underlying.missing(index(row)) else NA
  def value(row: Int): A = underlying.value(index(row))
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
  def missing(row: Int): Missing = if (bits(row)) underlying.missing(row) else NA
  def value(row: Int): A = underlying.value(row)
}

final class CastColumn[A: Typeable](col: Column[_]) extends Column[A] {
  def exists(row: Int): Boolean = col.exists(row) && col.apply(row).cast[A].isDefined
  def missing(row: Int): Missing = if (!col.exists(row)) col.missing(row) else NM
  def value(row: Int): A = col.value(row).cast[A].get
}

final class InfiniteColumn[A](f: Int => A) extends Column[A] {
  def exists(row: Int): Boolean = true
  def missing(row: Int): Missing = NA
  def value(row: Int): A = f(row)
}

final class DenseColumn[A](naValues: BitSet, nmValues: BitSet, values: Array[A]) extends Column[A] {
  private final def valid(row: Int) = row >= 0 && row < values.length
  def exists(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def missing(row: Int): Missing = if (nmValues(row)) NM else NA
  def value(row: Int): A = values(row)
}

final class CellColumn[A](values: IndexedSeq[Cell[A]]) extends Column[A] {
  private final def valid(row: Int): Boolean = row >= 0 && row < values.size
  def exists(row: Int): Boolean = valid(row) && !values(row).isMissing
  def missing(row: Int): Missing = if (valid(row)) {
    values(row) match {
      case Value(_) => throw new IllegalStateException()
      case (ms: Missing) => ms
    }
  } else NA
  def value(row: Int): A = values(row) match {
    case Value(x) => x
    case (_: Missing) => throw new IllegalStateException()
  }
}

final class MapColumn[A](values: Map[Int,A]) extends Column[A] {
  def exists(row: Int): Boolean = values contains row
  def missing(row: Int): Missing = NA
  def value(row: Int): A = values(row)
}

final class MergedColumn[A](left: Column[A], right: Column[A]) extends Column[A] {
  def exists(row: Int): Boolean = left.exists(row) || right.exists(row)
  def missing(row: Int): Missing = 
    if (!right.exists(row) && right.missing(row) == NM) NM else left.missing(row)
  def value(row: Int): A = if (right.exists(row)) right.value(row) else left.value(row)
}

final class WrappedColumn[A](f: Int => Cell[A]) extends Column[A] {
  def exists(row: Int): Boolean = !f(row).isMissing
  def missing(row: Int): Missing = f(row) match {
    case Value(_) => throw new IllegalArgumentException(s"row:$row is not missing")
    case (m: Missing) => m
  }
  def value(row: Int): A = f(row) match {
    case Value(a) => a
    case _ => throw new IllegalArgumentException(s"row:$row is missing (${missing(row)})")
  }
}

