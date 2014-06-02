package pellucid.pframe
package json

import spire.syntax.monoid._

trait JsonExtractor extends JsonModule {
  implicit object JsonValueColumnTyper extends ColumnTyper[JsonValue] {
    def cast(col: TypedColumn[_]): Column[JsonValue] = {
      val num = col.cast[BigDecimal] map JsonValue.jsonNumber
      val bool = col.cast[Boolean] map JsonValue.jsonBoolean
      val text = col.cast[String] map JsonValue.jsonString
      num |+| bool |+| text
    }
  }

  implicit object JsonValueRowExtractor extends RowExtractor[JsonValue, JsonPath, Variable] {
    type P = List[(JsonPath, Column[JsonValue])]
    def prepare[Row](frame: Frame[Row, JsonPath], cols: List[JsonPath]): Option[P] =
      Some(cols map { key =>
        key -> frame.column[JsonValue](key).column
      })

    def extract[Row](frame: Frame[Row, JsonPath], key: Row, row: Int, cols: P): Cell[JsonValue] =
      Cell.fromOption(inflate(for {
        (path, col) <- cols
        value <- col.foldRow(row)(Some(_), {
            case NA => None
            case NM => Some(JsonValue.jsonNull)
          })
      } yield (path -> value)))
  }

  def frameToJson(frame: Frame[_, JsonPath]): JsonValue =
    JsonValue.jsonArray(frame.columns.as[JsonValue].iterator.toVector flatMap (_._2.value))
}
