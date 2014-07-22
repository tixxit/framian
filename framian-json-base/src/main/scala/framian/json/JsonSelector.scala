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

import scala.reflect.ClassTag

import spire.algebra.Order

sealed trait JsonSelector

object JsonSelector {
  final case object Select extends JsonSelector
  final case class Index(pos: Int, tail: JsonSelector) extends JsonSelector
  final case class Field(name: String, tail: JsonSelector) extends JsonSelector

  implicit object JsonSelectorOrder extends Order[JsonSelector] {
    def compare(lhs: JsonSelector, rhs: JsonSelector): Int = (lhs, rhs) match {
      case (Select, Select) => 0
      case (Select, _) => -1
      case (_, Select) => 1
      case (Index(i, lhs0), Index(j, rhs0)) =>
        val cmp = i - j
        if (cmp == 0) compare(lhs0, rhs0) else cmp
      case (Index(_, _), _) => -1
      case (_, Index(_, _)) => 1
      case (Field(n, lhs0), Field(m, rhs0)) =>
        val cmp = n compare m
        if (cmp == 0) compare(lhs0, rhs0) else cmp
    }
  }
}

/**
 * Provides a JsonPath implementation for JSON frameworks that don't have a
 * notion of paths/selectors.
 */
trait JsonSelectorModule extends JsonModule {
  import JsonSelector._

  type JsonPath = JsonSelector

  object JsonPath extends JsonPathCompanion {
    def root: JsonPath = Select
    def cons(fieldName: String, path: JsonPath): JsonPath = Field(fieldName, path)
    def cons(index: Int, path: JsonPath): JsonPath = Index(index, path)
    def uncons[A](path: JsonPath)(z: => A, f: (String, JsonPath) => A, g: (Int, JsonPath) => A): A =
      path match {
        case Select => z
        case Field(key, tail) => f(key, tail)
        case Index(idx, tail) => g(idx, tail)
      }
  }

  val JsonPathOrder = JsonSelectorOrder
  val JsonPathClassTag = implicitly[ClassTag[JsonPath]]
}
