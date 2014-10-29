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
package json

import scala.annotation.tailrec
import scala.collection.mutable.BitSet
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.syntax.monoid._

import framian.column.Mask

private[json] sealed trait JsonColumn {
  import JsonColumn._

  def pos: Int

  final def toColumn: UntypedColumn = {
    val text = Column.newBuilder[String]
    val nums = Column.newBuilder[BigDecimal]
    val bool = Column.newBuilder[Boolean]

    @tailrec
    def loop(col: JsonColumn): UntypedColumn = {
      col match {
        case Start =>
          TypedColumn(text.result()) orElse
          TypedColumn(nums.result()) orElse
          TypedColumn(bool.result())
        case Text(pos, value, prev) =>
          text.addValue(value)
          nums.addNA()
          bool.addNA()
          loop(prev)
        case Number(pos, value, prev) =>
          text.addNA()
          nums.addValue(value)
          bool.addNA()
          loop(prev)
        case Bool(pos, value, prev) =>
          text.addNA()
          nums.addNA()
          bool.addValue(value)
          loop(prev)
      }
    }

    loop(this)
  }
}

private[json] object JsonColumn {
  case object Start extends JsonColumn { val pos = -1 }
  case class Text(pos: Int, value: String, prev: JsonColumn) extends JsonColumn
  case class Number(pos: Int, value: BigDecimal, prev: JsonColumn) extends JsonColumn
  case class Bool(pos: Int, value: Boolean, prev: JsonColumn) extends JsonColumn
}
