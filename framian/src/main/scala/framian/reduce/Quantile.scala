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
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

final class Quantile[A: Field: Order: ClassTag](percentiles: Seq[Double]) extends SimpleReducer[A, List[(Double, A)]] {
  require(percentiles.forall(p => p >= 0.0 && p <= 1.0), "percentile must lie in [0,1]")

  def reduce(data: Array[A]): Cell[List[(Double, A)]] =
    if (data.length == 0 && percentiles.length > 0) NA
    else Value(quantiles(data))

  def quantiles(s: Array[A]): List[(Double, A)] = {
    val as = s.sorted

    percentiles.map({ p =>
      val i = p * (as.length - 1)
      val lb = i.toInt
      val ub = math.ceil(i).toInt
      val w = i - lb
      val value = as(lb) * (1 - w) + as(ub) * w
      p -> value
    })(collection.breakOut)
  }
}
