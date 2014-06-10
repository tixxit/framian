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

import spire.algebra.Order
import spire.syntax.order._

final class Max[A: Order] extends Reducer[A, A] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop0(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        loop1(i + 1, column.valueAt(row))
      } else if (column.nonValueAt(row) == NA) {
        loop0(i + 1)
      } else {
        NM
      }
    } else NA

    @tailrec def loop1(i: Int, max: A): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        val value = column.valueAt(row)
        loop1(i + 1, if (value > max) value else max)
      } else if (column.nonValueAt(row) == NA) {
        loop1(i + 1, max)
      } else {
        NM
      }
    } else Value(max)

    loop0(start)
  }
}
