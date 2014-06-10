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

import scala.reflect.ClassTag

import spire.algebra.Order

trait JsonModule {
  type JsonValue
  def JsonValue: JsonValueCompanion

  type JsonError
  def JsonError: JsonErrorCompanion

  type JsonPath
  def JsonPath: JsonPathCompanion
  implicit def JsonPathOrder: Order[JsonPath]
  implicit def JsonPathClassTag: ClassTag[JsonPath]

  def visitJson[A](visitor: JsonVisitor[A])(json: JsonValue): A

  def parseJson(jsonStr: String): Either[JsonError, JsonValue]

  def parseJsonSeq(jsonStr: String): Either[JsonError, Seq[JsonValue]] =
    parseJson(jsonStr).right flatMap visitJson(SeqExtractor)

  trait JsonValueCompanion {
    def jsonObject(values: Seq[(String, JsonValue)]): JsonValue
    def jsonArray(values: Seq[JsonValue]): JsonValue
    def jsonString(value: String): JsonValue
    def jsonNumber(value: BigDecimal): JsonValue
    def jsonBoolean(value: Boolean): JsonValue
    def jsonNull: JsonValue
  }

  trait JsonVisitor[A] {
    def jsonObject(values: Iterable[(String, JsonValue)]): A
    def jsonArray(values: Seq[JsonValue]): A
    def jsonString(value: String): A
    def jsonNumber(value: BigDecimal): A
    def jsonBoolean(value: Boolean): A
    def jsonNull(): A
  }

  trait JsonPathCompanion {
    def root: JsonPath

    def cons(fieldName: String, path: JsonPath): JsonPath
    def cons(index: Int, path: JsonPath): JsonPath
    def uncons[A](path: JsonPath)(z: => A, f: (String, JsonPath) => A, g: (Int, JsonPath) => A): A
    def isEmpty(path: JsonPath): Boolean = uncons(path)(true, (_, _) => false, (_, _) => false)
  }

  implicit final class JsonPathOps(path: JsonPath) {
    def :: (fieldName: String): JsonPath = JsonPath.cons(fieldName, path)
    def :: (index: Int): JsonPath = JsonPath.cons(index, path)
    def uncons[A](z: => A, f: (String, JsonPath) => A, g: (Int, JsonPath) => A): A = JsonPath.uncons(path)(z, f, g)
    def isEmpty: Boolean = JsonPath.isEmpty(path)
  }

  trait JsonErrorCompanion {
    def invalidJson(message: String): JsonError
    def ioError(e: Exception): JsonError
  }

  /**
   * Inflates a JSON object from a set of paths and values. These paths/values
   * must be consistent, otherwise `None` will be returned.
   */
  def inflate(kvs: List[(JsonPath, JsonValue)]): Option[JsonValue] = {
    def sequence[A](xs: List[Option[A]], acc: Option[List[A]] = None): Option[List[A]] =
      xs.foldLeft(Some(Nil): Option[List[A]]) { (acc, x) =>
        acc flatMap { ys => x map (_ :: ys) }
      }

    def makeArray(fields: List[(Int, JsonValue)]): JsonValue = {
      val size = fields.map(_._1).max
      val elems = fields.foldLeft(Vector.fill(size)(JsonValue.jsonNull)) { case (xs, (i, x)) =>
        xs.updated(i, x)
      }
      JsonValue.jsonArray(elems)
    }

    def obj(kvs: List[(JsonPath, JsonValue)]): Option[JsonValue] = {
      val fields: Option[List[(String, (JsonPath, JsonValue))]] = sequence(kvs map { case (path, value) =>
        path.uncons(None, (key, path0) => Some((key, (path0, value))), (_, _) => None)
      })
      fields flatMap { fields0 =>
        sequence(fields0.groupBy(_._1).toList map { case (key, kvs0) =>
          inflate(kvs0 map (_._2)) map (key -> _)
        }) map JsonValue.jsonObject
      }
    }

    def arr(kvs: List[(JsonPath, JsonValue)]): Option[JsonValue] = {
      val fields: Option[List[(Int, (JsonPath, JsonValue))]] = sequence(kvs map { case (path, value) =>
        path.uncons(None, (_, _) => None, (idx, path0) => Some((idx, (path0, value))))
      })
      fields flatMap { fields0 =>
        sequence(fields0.groupBy(_._1).toList map { case (idx, kvs0) =>
          inflate(kvs0 map (_._2)) map (idx -> _)
        }) map makeArray
      }
    }

    kvs match {
      case Nil => None
      case (path, value) :: Nil if path.isEmpty => Some(value)
      case _ => obj(kvs) orElse arr(kvs)
    }
  }

  private object SeqExtractor extends JsonVisitor[Either[JsonError, Seq[JsonValue]]] {
     def error(tpe: String): Either[JsonError, Seq[JsonValue]] =
       Left(JsonError.invalidJson(s"Expected JSON array, but found ${tpe}."))

     def jsonObject(values: Iterable[(String, JsonValue)]): Either[JsonError, Seq[JsonValue]] = error("object")
     def jsonArray(values: Seq[JsonValue]): Either[JsonError, Seq[JsonValue]] = Right(values)
     def jsonString(value: String): Either[JsonError, Seq[JsonValue]] = error("string")
     def jsonNumber(value: BigDecimal): Either[JsonError, Seq[JsonValue]] = error("number")
     def jsonBoolean(value: Boolean): Either[JsonError, Seq[JsonValue]] = error("boolean")
     def jsonNull(): Either[JsonError, Seq[JsonValue]] = error("null")
  }
}
