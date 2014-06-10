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
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

final class Quantile[A: Field: Order: ClassTag](probabilities: Seq[Double]) extends SimpleReducer[A, List[(Double, A)]] {

  def reduce(data: Array[A]): Cell[List[(Double, A)]] =
    if (data.length == 0 && probabilities.length > 0) NA
    else Value(quantiles(data))

  def quantiles(s: Array[A]): List[(Double, A)] = {
    def interpolate(x: Double, pos: (Double, A), pos1: (Double, A)) =
      if (pos == pos1) Field[A].zero
      else pos._2 + (pos1._2 - pos._2) * (x - pos._1) / (pos1._1 - pos._1)

    val points = s.sorted
    val segments = points.length
    val segmentSize = (1d / segments)
    val segmentMedian = segmentSize / 2d
    val pointsWithQuantile = points.zipWithIndex.map { case (y, i) =>
      val start = i * segmentSize
      val position = start + segmentMedian
      (position, y)
    }

    val (_, probabilityRanges) =
      probabilities.sorted.foldLeft((pointsWithQuantile, List[(Double, A)]())) {
        case ((remainingPoints, accum), newProbability) =>
          val rangeIndex = remainingPoints.indexWhere(_._1 > newProbability)
          val rangeStart =
            if (rangeIndex == -1) 0
            else if (rangeIndex == (remainingPoints.length - 1) && remainingPoints.length > 1) (rangeIndex - 1)
            else rangeIndex
          val rangeEnd = if (remainingPoints.length <= 1) rangeStart else rangeStart + 1
          val (_, remainder) = remainingPoints.splitAt(rangeStart)

          (remainder,
           (newProbability,
            interpolate(newProbability, remainingPoints(rangeStart), remainingPoints(rangeEnd))) :: accum)
      }

    probabilityRanges.sortBy(_._1)
  }
}
