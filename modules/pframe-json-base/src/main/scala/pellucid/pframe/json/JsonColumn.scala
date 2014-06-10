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
package json

import scala.annotation.tailrec
import scala.collection.mutable.BitSet
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.syntax.monoid._

private[json] sealed trait JsonColumn {
  import JsonColumn._

  def pos: Int

  final def toColumn: UntypedColumn = {
    @tailrec
    def loop(col: JsonColumn, text: Buffer[String], nums: Buffer[BigDecimal], bools: Buffer[Boolean]): UntypedColumn = {
      col match {
        case Start =>
          text.toColumn |+| nums.toColumn |+| bools.toColumn
        case Text(pos, value, prev) =>
          text.set(pos, value)
          loop(prev, text, nums, bools)
        case Number(pos, value, prev) =>
          nums.set(pos, value)
          loop(prev, text, nums, bools)
        case Bool(pos, value, prev) =>
          bools.set(pos, value)
          loop(prev, text, nums, bools)
      }
    }

    loop(this, Buffer.create(), Buffer.create(), Buffer.create())
  }
}

private[json] object JsonColumn {
  case object Start extends JsonColumn { val pos = -1 }
  case class Text(pos: Int, value: String, prev: JsonColumn) extends JsonColumn
  case class Number(pos: Int, value: BigDecimal, prev: JsonColumn) extends JsonColumn
  case class Bool(pos: Int, value: Boolean, prev: JsonColumn) extends JsonColumn

  private final class Buffer[A: ClassTag: TypeTag](bitset: BitSet, var values: Array[A]) {
    def set(pos: Int, value: A): Unit = {
      values = if (null == values) new Array[A](pos + 1) else values
      values(pos) = value
      bitset.update(pos, true)
    }

    def toColumn: UntypedColumn =
      if (values != null) TypedColumn(Column.fromArray(values).mask(bitset))
      else UntypedColumn.empty
  }

  private object Buffer {
    def create[A: ClassTag: TypeTag]() = new Buffer(new BitSet, null)
  }
}
