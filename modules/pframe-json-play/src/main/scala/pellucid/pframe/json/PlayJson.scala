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

import java.io.IOException

import scala.annotation.tailrec
import scala.reflect.ClassTag

import play.api.libs.json._

import spire.algebra.Order

object PlayJson extends JsonModule with JsonLoader with JsonExtractor {
  type JsonValue = JsValue

  object JsonValue extends JsonValueCompanion {
    def jsonObject(values: Seq[(String, JsonValue)]) = JsObject(values)
    def jsonArray(values: Seq[JsonValue]) = JsArray(values)
    def jsonString(value: String) = JsString(value)
    def jsonNumber(value: BigDecimal) = JsNumber(value)
    def jsonBoolean(value: Boolean) = JsBoolean(value)
    def jsonNull = JsNull
  }

  def parseJson(jsonStr: String): Either[JsonError, JsonValue] = try {
    Right(Json.parse(jsonStr))
  } catch { case (e: IOException) =>
    Left(JsonError(e.getMessage, e))
  }

  final case class JsonError(message: String = null, cause: Throwable = null) extends Exception(message, cause)

  object JsonError extends JsonErrorCompanion {
    def invalidJson(message: String): JsonError = JsonError(message)
    def ioError(e: Exception): JsonError = JsonError(cause = e)
  }

  type JsonPath = JsPath

  object JsonPath extends JsonPathCompanion {
    def root: JsonPath = JsPath
    def cons(fieldName: String, path: JsonPath): JsonPath =
      JsPath(KeyPathNode(fieldName) :: path.path)
    def cons(index: Int, path: JsonPath): JsonPath =
      JsPath(IdxPathNode(index) :: path.path)
    def uncons[A](path: JsonPath)(z: => A, f: (String, JsonPath) => A, g: (Int, JsonPath) => A): A =
      path.path match {
        case Nil => z
        case KeyPathNode(k) :: path0 => f(k, JsPath(path0))
        case IdxPathNode(i) :: path0 => g(i, JsPath(path0))
        case RecursiveSearch(_) :: _ => ???
      }
  }

  val JsonPathClassTag = implicitly[ClassTag[JsonPath]]
  object JsonPathOrder extends Order[JsPath] {
    @tailrec
    def loop(lhs: List[PathNode], rhs: List[PathNode]): Int = (lhs, rhs) match {
      case (Nil, Nil) => 0
      case (Nil, _) => -1
      case (_, Nil) => 1
      case (IdxPathNode(i) :: lhs0, IdxPathNode(j) :: rhs0) =>
        val cmp = i - j
        if (cmp == 0) loop(lhs0, rhs0) else cmp
      case (IdxPathNode(_) :: _, _) => -1
      case (_, IdxPathNode(_) :: _) => 1
      case (KeyPathNode(n) :: lhs0, KeyPathNode(m) :: rhs0) =>
        val cmp = n compare m
        if (cmp == 0) loop(lhs0, rhs0) else cmp
      case (KeyPathNode(_) :: _, _) => -1
      case (_, KeyPathNode(_) :: _) => 1
      case (RecursiveSearch(n) :: lhs0, RecursiveSearch(m) :: rhs0) =>
        val cmp = n compare m
        if (cmp == 0) loop(lhs0, rhs0) else cmp
    }

    def compare(lhs: JsPath, rhs: JsPath): Int = loop(lhs.path, rhs.path)
  }

  def visitJson[A](visitor: JsonVisitor[A])(json: JsonValue): A = json match {
    case JsArray(arr) => visitor.jsonArray(arr)
    case JsBoolean(x) => visitor.jsonBoolean(x)
    case JsNull => visitor.jsonNull()
    case JsNumber(x) => visitor.jsonNumber(x)
    case JsObject(obj) => visitor.jsonObject(obj)
    case JsString(x) => visitor.jsonString(x)
    case (err: JsUndefined) =>
      throw new IllegalArgumentException(s"Invalid JSON value: ${err.error}")
  }
}
