/*  _____                    _
 * |  ___| __ __ _ _ __ ___ (_) __ _ _ __
 * | |_ | '__/ _` | '_ ` _ \| |/ _` | '_ \
 * |  _|| | | (_| | | | | | | | (_| | | | |
 * |_|  |_|  \__,_|_| |_| |_|_|\__,_|_| |_|
 *
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package framian
package columns

import language.experimental.macros

import scala.reflect.ClassTag
import scala.collection.mutable
import scala.collection.immutable.BitSet
import scala.{ specialized => spec }

import spire.algebra._

import shapeless._
import shapeless.syntax.typeable._

private[framian] final class EmptyColumn[A](nonValue: NonValue) extends Column[A] {
  def isValueAt(row: Int): Boolean = false
  def nonValueAt(row: Int): NonValue = nonValue
  def valueAt(row: Int): A = throw new UnsupportedOperationException()
}

private[framian] final class ConstColumn[@spec(Int,Long,Float,Double) A](value: A) extends Column[A] {
  def isValueAt(row: Int): Boolean = true
  def nonValueAt(row: Int): NonValue = throw new UnsupportedOperationException()
  def valueAt(row: Int): A = value
}

private[framian] final class SetNAColumn[A](na: Int, underlying: Column[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = row != na && underlying.isValueAt(row)
  def nonValueAt(row: Int): NonValue = if (row == na) NA else underlying.nonValueAt(row)
  def valueAt(row: Int): A = underlying.valueAt(row)
}

private[framian] final class ContramappedColumn[A](f: Int => Int, underlying: Column[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = underlying.isValueAt(f(row))
  def nonValueAt(row: Int): NonValue = underlying.nonValueAt(f(row))
  def valueAt(row: Int): A = underlying.valueAt(f(row))
}

private[framian] final class ReindexColumn[A](index: Array[Int], underlying: Column[A]) extends Column[A] {
  @inline private final def valid(row: Int) = row >= 0 && row < index.length
  def isValueAt(row: Int): Boolean = valid(row) && underlying.isValueAt(index(row))
  def nonValueAt(row: Int): NonValue = if (valid(row)) underlying.nonValueAt(index(row)) else NA
  def valueAt(row: Int): A = underlying.valueAt(index(row))
}

private[framian] final class ShiftColumn[A](shift: Int, underlying: Column[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = underlying.isValueAt(row - shift)
  def nonValueAt(row: Int): NonValue = underlying.nonValueAt(row - shift)
  def valueAt(row: Int): A = underlying.valueAt(row - shift)
}

private[framian] final class MappedColumn[A, B](f: A => B, underlying: Column[A]) extends Column[B] {
  def isValueAt(row: Int): Boolean = underlying.isValueAt(row)
  def nonValueAt(row: Int): NonValue = underlying.nonValueAt(row)
  def valueAt(row: Int): B = f(underlying.valueAt(row))
}

private[framian] final class FilteredColumn[A](f: A => Boolean, underlying: Column[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = underlying.isValueAt(row) && f(underlying.valueAt(row))
  def nonValueAt(row: Int): NonValue = if (underlying.isValueAt(row)) NA else underlying.nonValueAt(row)
  def valueAt(row: Int): A = underlying.valueAt(row)
}

private[framian] final class MaskedColumn[A](bits: Int => Boolean, underlying: Column[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = underlying.isValueAt(row) && bits(row)
  def nonValueAt(row: Int): NonValue = if (bits(row)) underlying.nonValueAt(row) else NA
  def valueAt(row: Int): A = underlying.valueAt(row)
}

private[framian] final class CastColumn[A: Typeable](col: Column[_]) extends Column[A] {
  def isValueAt(row: Int): Boolean = col.isValueAt(row) && col.apply(row).cast[A].isDefined
  def nonValueAt(row: Int): NonValue = if (!col.isValueAt(row)) col.nonValueAt(row) else NM
  def valueAt(row: Int): A = col.valueAt(row).cast[A].get
}

private[framian] final class InfiniteColumn[A](f: Int => A) extends Column[A] {
  def isValueAt(row: Int): Boolean = true
  def nonValueAt(row: Int): NonValue = NA
  def valueAt(row: Int): A = f(row)
}

private[framian] final class DenseColumn[A](naValues: BitSet, nmValues: BitSet, values: Array[A]) extends Column[A] {
  private final def valid(row: Int) = row >= 0 && row < values.length
  def isValueAt(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def nonValueAt(row: Int): NonValue = if (nmValues(row)) NM else NA
  def valueAt(row: Int): A = values(row)
}

private[framian] final class CellColumn[A](values: IndexedSeq[Cell[A]]) extends Column[A] {
  private final def valid(row: Int): Boolean = row >= 0 && row < values.size
  def isValueAt(row: Int): Boolean = valid(row) && values(row).isValue
  def nonValueAt(row: Int): NonValue = if (valid(row)) {
    values(row) match {
      case Value(_) => throw new IllegalStateException()
      case (ms: NonValue) => ms
    }
  } else NA
  def valueAt(row: Int): A = values(row) match {
    case Value(x) => x
    case (_: NonValue) => throw new IllegalStateException()
  }
}

private[framian] final class MapColumn[A](values: Map[Int,A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = values contains row
  def nonValueAt(row: Int): NonValue = NA
  def valueAt(row: Int): A = values(row)
}

private[framian] final class MergedColumn[A](left: Column[A], right: Column[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = left.isValueAt(row) || right.isValueAt(row)
  def nonValueAt(row: Int): NonValue =
    if (!right.isValueAt(row) && right.nonValueAt(row) == NM) NM else left.nonValueAt(row)
  def valueAt(row: Int): A = if (right.isValueAt(row)) right.valueAt(row) else left.valueAt(row)
}

private[framian] final class WrappedColumn[A](f: Int => Cell[A]) extends Column[A] {
  def isValueAt(row: Int): Boolean = f(row).isValue
  def nonValueAt(row: Int): NonValue = f(row) match {
    case v @ Value(_) => throw new IllegalArgumentException(s"expected a non value at row:$row, found ($v)!")
    case (m: NonValue) => m
  }
  def valueAt(row: Int): A = f(row) match {
    case Value(a) => a
    case _ => throw new IllegalArgumentException(s"expected a value at row:$row, found (${nonValueAt(row)})")
  }
}

private[framian] final class ZipMapColumn[A, B, C](f: (A, B) => C, lhs: Column[A], rhs: Column[B])
    extends Column[C] {
  def isValueAt(row: Int): Boolean = lhs.isValueAt(row) && rhs.isValueAt(row)
  def nonValueAt(row: Int): NonValue =
    if (lhs.isValueAt(row)) rhs.nonValueAt(row)
    else if (rhs.isValueAt(row)) lhs.nonValueAt(row)
    else if (lhs.nonValueAt(row) == NM) NM
    else rhs.nonValueAt(row)
  def valueAt(row: Int): C = f(lhs.valueAt(row), rhs.valueAt(row))
}
