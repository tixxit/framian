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

import scala.reflect.ClassTag
import scala.collection.mutable
import scala.collection.immutable.BitSet
import scala.{ specialized => spec }

import framian.columns.DenseColumn

final class ColumnBuilder[@spec(Int,Long,Float,Double) V: ClassTag] extends mutable.Builder[Cell[V], Column[V]] {
  private var empty: V = _
  private var size: Int = 0
  private final val bldr = mutable.ArrayBuilder.make[V]()
  private final val naValues = new mutable.BitSet
  private final val nmValues = new mutable.BitSet

  def addValue(v: V): Unit = { bldr += v; size += 1 }
  def addNA(): Unit = { naValues.add(size); addValue(empty) }
  def addNM(): Unit = { nmValues.add(size); addValue(empty) }

  def addNonValue(nonValue: NonValue): Unit = nonValue match {
    case NA => addNA()
    case NM => addNM()
  }

  def +=(elem: Cell[V]): this.type = {
    elem match {
      case Value(v) => addValue(v)
      case NA => addNA()
      case NM => addNM()
    }
    this
  }

  def clear(): Unit = {
    size = 0
    bldr.clear()
    naValues.clear()
    nmValues.clear()
  }

  override def sizeHint(size: Int): Unit = {
    bldr.sizeHint(size)
    naValues.sizeHint(size)
    nmValues.sizeHint(size)
  }

  def result(): Column[V] = new DenseColumn(naValues.toImmutable, nmValues.toImmutable, bldr.result())
}

