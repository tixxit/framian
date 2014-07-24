package framian
package csv

/**
 * A single row in a CSV file.
 */
final class CsvRow(val cells: Vector[CsvCell]) extends AnyVal {
  def text(format: CsvFormat): Vector[String] = cells.map(_ render format)

  def render(format: CsvFormat): String =
    cells.iterator map (_ render format) mkString format.separator

  override def toString: String =
    cells.mkString("CsvRow(", ", ", ")")
}

object CsvRow extends (Vector[CsvCell] => CsvRow) {
  def apply(cells: Vector[CsvCell]): CsvRow = new CsvRow(cells)

  implicit def csvRowExtractor[Col]: RowExtractor[CsvRow, Col, Variable] =
    RowExtractor.collectionOf[Vector, CsvCell, Col].map { cells =>
      CsvRow(cells.map(_.fold[CsvCell](CsvCell.Empty, CsvCell.Invalid)(cell => cell)))
    }
}
