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

/**
 * A low level trait for implementing reductions.
 *
 * TODO: All reducers should return Cell[B].
 *
 * @tparam A the input type of the reducer, which is the value type of the input [[Column]]
 * @tparam B the ouput type of the reducer, which is the value type of the output [[Cell]]
 */
trait Reducer[-A, +B] {

  /** Reduce the given column of values to a cell using only the
    * indexes in given array slice.
    *
    * `indices`, `start`, and `end` represent an array slice. The
    * following must hold:
    *
    *  - `0 <= start`
    *  - `end <= indices.length`
    *  - `start <= end`
    *
    * and the slice is `[start:end)`, inclusive of `start` and
    * exclusive of `end`.
    *
    * Let `i: Int` where `start <= i` and `i < end`, then
    *
    * {{{
    * column(indices(i)) match {
    *     case Value(v) => // 1.
    *     case NA => // 2.
    *     case NM => // 3.
    * }
    * }}}
    *
    *  1. In the case of a [[Value]], the value should be included in the reduction.
    *  1. In the case of [[NA]], the reduction should skip.
    *  1. In the case of [[NM]], the reduction should terminate with [[NM]].
    *
    *
    * @param column the column of values is the source of the reduction
    * @param indices the array of column indexes to reduce over
    * @param start the start of the array slice on the indices
    * @param end the end of the array slice on the indices
    * @return the result of the reduction as a [[Cell]]
    */
  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[B]
}
