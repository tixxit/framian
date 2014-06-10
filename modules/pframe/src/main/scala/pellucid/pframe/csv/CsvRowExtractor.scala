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
package csv

import spire.syntax.monoid._
import org.joda.time._

sealed abstract class CsvRowDelim(val value: String)
object CsvRowDelim {
  case object Unix extends CsvRowDelim("\n")
  case object Windows extends CsvRowDelim("\r\n")
}

case class CsvFormat(
  separator: String,
  quote: String = "\"",
  quoteEscape: String = "\"",
  empty: String = "NA",
  invalid: String = "NM",
  header: Boolean = true,
  rowDelim: CsvRowDelim = CsvRowDelim.Windows
) {
  val escapedQuote = quoteEscape + quote

  /**
   * Replaces all instances of \r\n with \n, then escapes all quotes and wraps
   * the string in quotes.
   */
  def escape(text: String): String = {
    val text0 = text.replace("\r\n", "\n").replace(quote, escapedQuote)
    s"${quote}$text0${quote}"
  }

  /**
   * Renders a single cell of data, escaping the value if necessary.
   */
  def render(text: String): String = {
    if ((text contains '\n') ||
        (text contains separator) ||
        (text contains quote)) escape(text)
    else text
  }
}

object CsvFormat {
  val CSV = CsvFormat(",")
  val TSV = CsvFormat("\t")
}

sealed abstract class CsvCell(val render: CsvFormat => String)

object CsvCell {
  case class Number(num: BigDecimal) extends CsvCell(_ render num.toString)
  case class Text(value: String) extends CsvCell(_ render value)
  case object Empty extends CsvCell(_.empty)
  case object Invalid extends CsvCell(_.invalid)

  def fromNonValue(nonValue: NonValue): CsvCell = nonValue match {
    case NA => Empty
    case NM => Invalid
  }

  implicit object CsvCellColumnTyper extends ColumnTyper[CsvCell] {
    def cast(col: TypedColumn[_]): Column[CsvCell] = {
      val num = col.cast[BigDecimal] map (Number(_): CsvCell)
      val text = col.cast[String] map (Text(_): CsvCell)
      val date = col.cast[LocalDate] map { date: LocalDate => Text(date.toString): CsvCell }
      num |+| text |+| date
    }
  }
}

/**
 * A single row in a CSV file.
 */
final class CsvRow(val cells: List[CsvCell]) extends AnyVal {
  def render(format: CsvFormat): String =
    cells.iterator map (_ render format) mkString format.separator
}

object CsvRow {
  def apply(cells: List[CsvCell]): CsvRow = new CsvRow(cells)

  implicit object CsvRowExtractor extends RowExtractor[CsvRow, String, Variable] {
    type P = List[Column[CsvCell]]
    def prepare[Row](frame: Frame[Row, String], cols: List[String]): Option[List[Column[CsvCell]]] =
      Some(cols map { key => frame.column[CsvCell](key)(CsvCell.CsvCellColumnTyper).column })
    def extract[Row](frame: Frame[Row, String], key: Row, row: Int, cols: List[Column[CsvCell]]): Cell[CsvRow] =
      Value(CsvRow(cols map { _.foldRow(row)(a => a, CsvCell.fromNonValue) }))
  }
}

final case class Csv(header: Option[List[String]], rows: List[CsvRow]) {
  def render(format: CsvFormat): String = {
    val full = header filter (_ => format.header) map { headings =>
      CsvRow(headings map (CsvCell.Text(_))) :: rows
    } getOrElse rows
    full.iterator map (_ render format) mkString format.rowDelim.value
  }
}

object Csv {
  def fromFrame(frame: Frame[_, String]): Csv = {
    val header = frame.colIndex.toList map (_._1)
    val rows = frame.columns.as[CsvRow].denseIterator.map {
      case (_, row) => row
    } .toList
    Csv(Some(header), rows)
  }
}
