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

import java.util.concurrent.ConcurrentHashMap

private[framian] sealed trait MemoizingColumn[A] extends BoxedColumn[A] {
  private def eval: EvalColumn[A] = EvalColumn(apply _)
  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = eval.cellMap(f)
  def reindex(index: Array[Int]): Column[A] = eval.reindex(index)
  def force(len: Int): Column[A] = eval.force(len)
  def mask(mask: Mask): Column[A] = eval.mask(mask)
  def setNA(naRow: Int): Column[A] = eval.setNA(naRow)
  def memoize(optimistic: Boolean): Column[A] = this
  def orElse[A0 >: A](that: Column[A0]): Column[A0] = eval.orElse(that)
  def shift(n: Int): Column[A] = eval.shift(n)
}

private[framian] class OptimisticMemoizingColumn[A](get: Int => Cell[A]) extends MemoizingColumn[A] {
  private val cached: ConcurrentHashMap[Int, Cell[A]] = new ConcurrentHashMap()

  def apply(row: Int): Cell[A] = {
    if (!cached.containsKey(row))
      cached.putIfAbsent(row, get(row))
    cached.get(row)
  }
}

private[framian] class PessimisticMemoizingColumn[A](get: Int => Cell[A]) extends MemoizingColumn[A] {
  private val cached: ConcurrentHashMap[Int, Box] = new ConcurrentHashMap()

  def apply(row: Int): Cell[A] = {
    if (!cached.containsKey(row))
      cached.putIfAbsent(row, new Box(row))
    cached.get(row).cell
  }

  // A Box let's us do the double-checked locking per-value, rather than having
  // to lock the entire cache for the update.
  private class Box(row: Int) {
    @volatile var _cell: Cell[A] = null
    def cell: Cell[A] = {
      var result = _cell
      if (result == null) {
        synchronized {
          result = _cell
          if (result == null) {
            _cell = get(row); result = _cell
          }
        }
      }
      result
    }
  }
}

