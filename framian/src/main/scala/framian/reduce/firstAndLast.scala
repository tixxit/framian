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
package reduce

import scala.annotation.tailrec

final class First[A] extends Reducer[A, A] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      column(row) match {
        case NA => loop(i + 1)
        case first => first
      }
    } else NA

    loop(start)
  }
}

final class Last[A] extends Reducer[A, A] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int): Cell[A] = if (i >= start) {
      val row = indices(i)
      column(row) match {
        case NA => loop(i - 1)
        case last => last
      }
    } else NA

    loop(end - 1)
  }
}


final class FirstN[A](n: Int) extends Reducer[A, List[A]] {
  require(n > 0, s"new FirstN(n = $n), but n must be greater than 0")

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[List[A]] = {
    val rows = List.newBuilder[A]
    var k = 1

    val success = column.foreach(start, end, indices(_)) { (_, value) =>
      rows += value
      if (k == n)
        return Value(rows.result())
      k += 1
    }

    if (success) NA else NM
  }
}


final class LastN[A](n: Int) extends Reducer[A, List[A]] {
  require(n > 0, s"new LastN(n = $n), but n must be greater than 0")

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[List[A]] = {
    var rows = List.empty[A]
    var k = 1

    // TOODO: The end - i + start - 1 is rather unsatisifying.
    val success = column.foreach(start, end, { i => indices(end - i + start - 1) }) { (_, value) =>
      rows = value :: rows
      if (k == n)
        return Value(rows)
      k += 1
    }

    if (success) NA else NM
  }
}
