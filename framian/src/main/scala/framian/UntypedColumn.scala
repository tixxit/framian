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

import scala.reflect.{ ClassTag, classTag }

import spire.algebra._
import spire.syntax.monoid._

import shapeless._
import shapeless.syntax.typeable._

import framian.column._

/**
 * An abstraction for heterogeneously typed columns. We work with them by
 * casting to a real, typed column. Values that cannot be cast are treated as
 * `NM` (not meaningful) values.
 */
trait UntypedColumn extends ColumnLike[UntypedColumn] {
  def cast[A: ColumnTyper]: Column[A]
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
  def cast[A: ColumnTyper]: Column[A] = Column.Empty
  def mask(na: Mask): UntypedColumn = EmptyUntypedColumn
  def shift(rows: Int): UntypedColumn = EmptyUntypedColumn
  def reindex(index: Array[Int]): UntypedColumn = EmptyUntypedColumn
  def setNA(row: Int): UntypedColumn = EmptyUntypedColumn
}

case class TypedColumn[A](column: Column[A])(implicit val classTagA: ClassTag[A]) extends UntypedColumn {
  def cast[B](implicit typer: ColumnTyper[B]): Column[B] = typer.cast(this)
  def mask(na: Mask): UntypedColumn = TypedColumn(column.mask(na))
  def shift(rows: Int): UntypedColumn = TypedColumn(column.shift(rows))
  def reindex(index: Array[Int]): UntypedColumn = TypedColumn(column.reindex(index))
  def setNA(row: Int): UntypedColumn = TypedColumn(column.setNA(row))
}

case class MergedUntypedColumn(left: UntypedColumn, right: UntypedColumn) extends UntypedColumn {
  def cast[A: ColumnTyper]: Column[A] = left.cast[A] orElse right.cast[A]
  def mask(na: Mask): UntypedColumn = MergedUntypedColumn(left.mask(na), right.mask(na))
  def shift(rows: Int): UntypedColumn = MergedUntypedColumn(left.shift(rows), right.shift(rows))
  def reindex(index: Array[Int]): UntypedColumn = MergedUntypedColumn(left.reindex(index), right.reindex(index))
  def setNA(row: Int): UntypedColumn = MergedUntypedColumn(left.setNA(row), right.setNA(row))
}

case class ConcatColumn(col0: UntypedColumn, col1: UntypedColumn, offset: Int) extends UntypedColumn {
  def cast[A: ColumnTyper]: Column[A] = ???
    // col0.cast[A].force(offset) orElse col1.cast[A].shift(offset).mask(Mask.from
  def mask(na: Mask): UntypedColumn =
    ConcatColumn(col0.mask(na), col1.mask(na.filter(_ >= offset).map(_ - offset)), offset)
  def shift(rows: Int): UntypedColumn =
    ConcatColumn(col0.shift(rows), col1.shift(rows), offset + rows)
  def reindex(index: Array[Int]): UntypedColumn = {
    val index0 = index.map { row =>
      if (row < offset) row else offset
    }
    val index1 = index.map { row =>
      if (row < offset) offset - 1 else row
    }
    MergedUntypedColumn(col0.setNA(offset).reindex(index0), col1.setNA(offset - 1).reindex(index1))
  }
  def setNA(row: Int): UntypedColumn =
    if (row < offset) ConcatColumn(col0.setNA(row), col1, offset)
    else ConcatColumn(col0, col1.setNA(row - offset), offset)
}
