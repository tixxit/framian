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

package pellucid.pframe
package reduce

import scala.annotation.tailrec


/** A [[Reducer]] that exististentially quantifies a predicate
  * as a reduction over a collection of [[Cell]]s.
  *
  * This reducer will return true upon encountering the first value
  * that is available and meaningful ''and'' applying the predicate
  * `p` to that value returns true. Otherwise, returns false.
  *
  * This reducer is unusual in that it will ignore [[NonValue]]s, in
  * particular, it will not propogate [[NM]]. If the predicate can be
  * satisfied by a value that is available and meaningful elsewhere in
  * the collection, then this reduction should still return true.
  *
  * This reducer will only traverse the entire collection if it never
  * encounters an available and meaningful value that satisfies the
  * predicate `p`.
  *
  * @example {{{
  * Series.empty[Int, Int].reduce(new Exists[Int](i => true)) == Value(false)
  *
  * Series(1 -> 1, 2 -> 2).reduce(new Exists[Int](i => i < 2)) == Value(true)
  *
  * Series.fromCells[Int, Int](1 -> NA, 2 -> 1).reduce(new Exists[Int](i => i < 2)) == Value(true)
  * Series.fromCells[Int, Int](1 -> NM, 2 -> 1).reduce(new Exists[Int](i => i < 2)) == Value(true)
  * }}}
  *
  * @note This reducer will always return precisely `Value[Boolean]`,
  *   rather than `Cell[Boolean]`. This in constrast to most reducers
  *   that will also return [[NonValue]]s.
  *
  * @tparam  A  the value type of the column to reduce.
  *
  * @constructor Create a existential quantifier reducer from a predicate `p`.
  * @param  p  the predicate to apply to the values of the column.
  * @see [[ForAll]]
  */
final class Exists[A](p: A => Boolean) extends Reducer[A, Boolean] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Value[Boolean] = {
    @tailrec def loop(i: Int): Value[Boolean] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        if (p(column.valueAt(row))) Value(true) else loop(i + 1)
      } else loop(i + 1)
    } else Value(false)

    loop(start)
  }
}


/** A [[Reducer]] that universally quantifies a predicate
  * as a reduction over a collection of [[Cell]]s.
  *
  * This reducer will return false upon encountering the first value
  * that is not meaningful, or the first value that is available and
  * meaningful ''and'' applying the predicate `p` to that value
  * returns false. Otherwise, returns true.
  *
  * This reducer does propogate [[NM]], in a sense, but the result is
  * `Value(false)` rather than `NM`. Unavailable values ([[NA]]) are
  * treated as the vaccuous case, so they will in count as a counter
  * example to the quantification.
  *
  * This reducer will only traverse the entire collection if it never
  * encounters a not meaningful value or a meaningful value that does
  * not satisfy the predicate `p`.
  *
  * @example {{{
  * Series.empty[Int, Int].reduce(new ForAll[Int](i => false)) == Value(true)
  *
  * Series(1 -> 1, 2 -> 2).reduce(new ForAll[Int](i => i < 3)) == Value(true)
  * Series(1 -> 1, 2 -> 2).reduce(new ForAll[Int](i => i < 2)) == Value(false)
  *
  * Series.fromCells[Int, Int](1 -> NA)        .reduce(new ForAll[Int](i => false)) == Value(true)
  * Series.fromCells[Int, Int](1 -> 1, 2 -> NM).reduce(new ForAll[Int](i => i < 2)) == Value(false)
  * }}}
  *
  * @note This reducer will always return precisely `Value[Boolean]`,
  *   rather than `Cell[Boolean]`. This in constrast to most reducers
  *   that will also return [[NonValue]]s.
  *
  * @tparam  A  the value type of the column to reduce.
  *
  * @constructor Create a universal quantifier reducer from a predicate `p`.
  * @param  p  the predicate to apply to the values of the column.
  * @see [[Exists]]
  */
final class ForAll[A](p: A => Boolean) extends Reducer[A, Boolean] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Value[Boolean] = {
    @tailrec def loop(i: Int): Value[Boolean] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        if (p(column.valueAt(row))) loop(i + 1) else Value(false)
      } else if (column.nonValueAt(row) == NA) {
        loop(i + 1)
      } else Value(false)
    } else Value(true)

    loop(start)
  }
}
