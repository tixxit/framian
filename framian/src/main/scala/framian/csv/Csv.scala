package framian
package csv

import spire.std.int._
import spire.std.string._

final case class Csv(format: CsvFormat, header: Option[Vector[String]], rows: Vector[Either[CsvError, CsvRow]]) {
  def toFrame: Frame[Int, Int] = {
    val validRows = rows.collect { case Right(row) => row }
    val cols = validRows.foldLeft(Map.empty[Int,(ColumnBuilder[BigDecimal],ColumnBuilder[String])]) { (acc0, row) =>
        row.cells.zipWithIndex.foldLeft(acc0) { case (acc, (cell, colIdx)) =>
          val (numCol, strCol) = acc.getOrElse(colIdx, (new ColumnBuilder[BigDecimal], new ColumnBuilder[String]))
          cell match {
            case CsvCell.Data(value) =>
              numCol += scala.util.Try(BigDecimal(value)).map(Value(_)).getOrElse(NA)
              strCol.addValue(value)
            case CsvCell.Empty =>
              numCol.addNA()
              strCol.addNA()
            case CsvCell.Invalid =>
              numCol.addNM()
              strCol.addNM()
          }
          acc + (colIdx -> (numCol, strCol))
        }
    }

    val columns = Column.fromMap(cols.map { case (col, (numCol, strCol)) =>
      col -> (TypedColumn(numCol.result()) orElse TypedColumn(strCol.result()))
    })

    Frame.fromColumns(
      Index(Array.range(0, validRows.size)),
      Index(Array.range(0, cols.size)),
      columns
    )
  }

  def toLabeledFrame: Frame[Int, String] = header map { header =>
    toFrame.withColIndex(Index.fromKeys(header: _*))
  } getOrElse ???

  override def toString: String = {
    val full = header filter (_ => format.header) map { headings =>
      Right(CsvRow(headings map (CsvCell.Data(_)))) +: rows
    } getOrElse rows

    full.iterator.
      collect { case Right(row) => row }.
      map(_ render format).
      mkString(format.rowDelim.value)
  }
}

object Csv {
  val BufferSize = 32 * 1024

  def empty(format: CsvFormat): Csv = Csv(format, None, Vector.empty)

  def fromFrame[Col](format: CsvFormat)(frame: Frame[_, Col]): Csv = {
    val header = if (format.header) {
      Some(frame.colIndex.toVector map (_._1.toString))
    } else {
      None
    }
    val rows = frame.get(Cols.all[Col].as[CsvRow]).denseIterator.map {
      case (_, row) => Right(row)
    }.toVector
    Csv(format, header, rows)
  }

  import java.nio.charset.{ Charset, StandardCharsets }
  import java.io.File
  import java.io.{ InputStream, FileInputStream }
  import java.io.{ Reader, InputStreamReader }

  def parseString(input: String): Csv =
    CsvParser(CsvFormat.guess(input)).parseString(input)

  def parseReader(reader: Reader): Csv = {
    val (format, reader0) = CsvFormat.guess(reader)
    CsvParser(format).parseReader(reader0)
  }

  def parseInputStream(is: InputStream, charset: Charset = StandardCharsets.UTF_8): Csv =
    parseReader(new InputStreamReader(is, charset))

  def parseFile(file: File, charset: Charset = StandardCharsets.UTF_8): Csv =
    parseInputStream(new FileInputStream(file), charset)

  def parse(filename: String, charset: Charset = StandardCharsets.UTF_8): Csv =
    parseFile(new File(filename), charset)
}
