package pellucid.pframe
package json

import scala.reflect.ClassTag

import spire.algebra.Order

sealed trait JsonSelector

object JsonSelector {
  final case object Select extends JsonSelector
  final case class Index(pos: Int, tail: JsonSelector) extends JsonSelector
  final case class Field(name: String, tail: JsonSelector) extends JsonSelector

  implicit object order extends Order[JsonSelector] {
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

  val JsonPathOrder = JsonSelector.order
  val JsonPathClassTag = implicitly[ClassTag[JsonPath]]
}
