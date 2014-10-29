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

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

import spire.syntax.cfor._

abstract class SimpleReducer[A: ClassTag, B] extends Reducer[A, B] {

  final def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[B] = {
    val bldr = ArrayBuilder.make[A]
    val success = column.foreach(start, end, indices(_)) { (_, a) => bldr += a }
    if (success) reduce(bldr.result()) else NM
  }

  def reduce(data: Array[A]): Cell[B]
}
