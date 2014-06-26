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

import scala.language.implicitConversions
import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

final class Median[A: Field: Order: ClassTag] extends SimpleReducer[A, A] {
  implicit def chooseRandomPivot(arr: Array[A]): A = arr(scala.util.Random.nextInt(arr.size))

  @tailrec def findKMedian(arr: Array[A], kOrValue: Either[Int, A], k2OrValue: Either[Int, A])
                          (implicit choosePivot: Array[A] => A): (A, A) = {
    val a = choosePivot(arr)
    (kOrValue, k2OrValue) match {
      case (Right(v1), Right(v2)) =>
        (v1, v2)
      case (Left(k), Left(k2)) =>
        val (s, b) = arr partition { x => a > x }
        if (s.size == k) findKMedian(arr, Right(a), Left(k2))
        else if (s.size == k2) findKMedian(arr, Left(k), Right(a))
        else if (s.isEmpty) {
          val (s, b) = arr partition (a == _)
          if (s.size > k && s.size > k2) (a, a)
          else if (s.size > k) findKMedian(arr, Right(a), Left(k2))
          else if (s.size > k2) findKMedian(arr, Left(k), Right(a))
          else findKMedian(b, Left(k - s.size), Left(k2 - s.size))
        } else if (s.size < k && s.size < k2) findKMedian(b, Left(k - s.size), Left(k2 - s.size))
          else if (s.size < k) findKMedian(b, Left(k - s.size), Left(k2))
          else if (s.size < k2) findKMedian(b, Left(k), Left(k2 - s.size))
        else findKMedian(s, Left(k), Left(k2))

      case (Left(k), Right(v)) =>
        val (s, b) = arr partition { x => a > x }
        if (s.size == k)  (a, v)
        else if (s.isEmpty) {
          val (s, b) = arr partition (a == _)
          if (s.size > k) (a, v)
          else findKMedian(b, Left(k - s.size), k2OrValue)
        } else if (s.size < k) findKMedian(b, Left(k - s.size), k2OrValue)
          else findKMedian(s, Left(k), k2OrValue)

      case (Right(v), Left(k)) =>
        val (s, b) = arr partition { x => a > x }
        if (s.size == k)  (v, a)
        else if (s.isEmpty) {
          val (s, b) = arr partition (a == _)
          if (s.size > k) (v, a)
          else findKMedian(b, kOrValue, Left(k - s.size))
        } else if (s.size < k) findKMedian(b, kOrValue, Left(k - s.size))
          else findKMedian(s, kOrValue, Left(k))
    }
  }

  def findMedian(arr: Array[A])(implicit choosePivot: Array[A] => A) = {
    if (arr.size % 2 == 0) {
      val (left, right) = findKMedian(arr, Left(arr.size / 2), Left((arr.size / 2) - 1))
      (left / 2) + (right / 2)
    } else findKMedian(arr, Left((arr.size - 1) / 2), Left((arr.size - 1) / 2))._1
  }

  // TODO: Use Spire's qselect/qmin instead?
  // def findMedianSpire(data: Array[A]): A = {
  //   val lower = data.qselect(data.size / 2)
  //   if (data.size % 2 == 0) {
  //     val upper = data.qselect(data.size / 2 + 1)
  //     (lower + upper) / 2
  //   } else {
  //     lower
  //   }
  // }

  def reduce(data: Array[A]): Cell[A] =
    if (data.isEmpty) NA else Value(findMedian(data))
}
