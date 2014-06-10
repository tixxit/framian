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
    JsonValue.jsonArray(frame.columns.as[JsonValue].toVector flatMap (_._2.value))
}
