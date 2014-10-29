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
package column

import spire.macros.{ Checked, ArithmeticOverflowException }

private[framian] case class EvalColumn[A](f: Int => Cell[A]) extends BoxedColumn[A] {
  override def apply(row: Int): Cell[A] = f(row)

  def cellMap[B](g: Cell[A] => Cell[B]): Column[B] = EvalColumn(f andThen g)

  def reindex(index: Array[Int]): Column[A] =
    DenseColumn.force(index andThen f, index.length)

  def force(len: Int): Column[A] =
    DenseColumn.force(f, len)

  def mask(mask: Mask): Column[A] = EvalColumn { row =>
    if (mask(row)) NA else f(row)
  }

  def setNA(naRow: Int): Column[A] = EvalColumn { row =>
    if (row == naRow) NA else f(row)
  }

  def memoize(optimistic: Boolean): Column[A] =
    if (optimistic) new OptimisticMemoizingColumn(f)
    else new PessimisticMemoizingColumn(f)

  def orElse[A0 >: A](that: Column[A0]): Column[A0] =
    EvalColumn { row =>
      f(row) match {
        case NM => that(row) match {
          case NA => NM
          case cell => cell
        }
        case NA => that(row)
        case cell => cell
      }
    }

  def shift(n: Int): Column[A] = EvalColumn { row =>
    try {
      f(Checked.minus(row, n))
    } catch { case (_: ArithmeticOverflowException) =>
      NA
    }
  }
}

