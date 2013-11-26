package pellucid.pframe
package json

import scala.reflect.ClassTag

import spire.algebra.Order

trait JsonModule {
  type JsonValue

  type JsonError
  def JsonError: JsonErrorCompanion

  type JsonPath
  def JsonPath: JsonPathCompanion
  implicit def JsonPathOrder: Order[JsonPath]
  implicit def JsonPathClassTag: ClassTag[JsonPath]

  def visitJson[A](visitor: JsonVisitor[A])(json: JsonValue): A

  def parse(jsonStr: String): Either[JsonError, JsonValue]

  def parseSeq(jsonStr: String): Either[JsonError, Seq[JsonValue]] =
    parse(jsonStr).right flatMap visitJson(SeqExtractor)

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

    def prepend(fieldName: String, path: JsonPath): JsonPath
    def prepend(index: Int, path: JsonPath): JsonPath
  }

  implicit final class JsonPathOps(path: JsonPath) {
    def :: (fieldName: String): JsonPath = JsonPath.prepend(fieldName, path)
    def :: (index: Int): JsonPath = JsonPath.prepend(index, path)
  }

  trait JsonErrorCompanion {
    def invalidJson(message: String): JsonError
    def ioError(e: Exception): JsonError
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
