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

final class Outliers[A: Field: Order: ClassTag](k: Double) extends SimpleReducer[A, (Option[A], Option[A])] {
  val quantiler = new Quantile[A](Seq(.25, .75))

  def reduce(data: Array[A]): Value[(Option[A], Option[A])] = {
    val (_, q1) :: (_, q3) :: Nil = quantiler.quantiles(data)

    val iqr = q3 - q1
    val lowerFence = q1 - (k * iqr)
    val upperFence = q3 + (k * iqr)

    val lowerOutliers = data.filter(_ <= lowerFence)
    val upperOutliers = data.filter(_ >= upperFence)

    Value((if (lowerOutliers.length > 0) Some(lowerFence) else None,
           if (upperOutliers.length > 0) Some(upperFence) else None))
  }
}
