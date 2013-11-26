package pellucid
package pframe

import java.util.Scanner
import java.io._

import scala.collection.mutable.ArrayBuffer

import scala.io.Source
import spire.math.Number
import spire.implicits._

object Utilities {

  /** Load a frame from CSV.
    *
    * @param delimiter how values in csv are delimited (tabs, commas, etc). defaults, per name of method, to comma.
    * @param columnIndex whether or not the first row is expected to indicate the column index of the frame
    * @param rowIndex whether or not the first column is expected to express the row index of the frame
    */
  def csv(
    location: String,
    delimiter: String = ",",
    quote: String = "\"",
    rowIndex: Option[Int] = None,
    columnIndex: Boolean = false,
    contentColumnsToParse: List[Int] = List()
  ) = {
    // sometimes you just want the bulk of the state-machine compiled for you...
    val stripWhitespaceCheckQuoteState =
      s"(?:[\\s]*($quote[^$quote]*$quote|[^\\s$quote]*)[\\s]*)|[\\s]*($quote[^$quote]*)".r
    val checkQuoteFinished = s"([^$quote]*)|([^$quote]*$quote)[\\s]*".r

    // assuming sorted list of desired values...
    def parseLine(line: String, columns: List[Int] = List()): ArrayBuffer[String] = {
      val lineScanner = new Scanner(line).useDelimiter(delimiter)
      var inQuote = false
      var position = 0
      val results = ArrayBuffer[String]()
      var quoteBuilder: StringBuilder = null

      while (lineScanner.hasNext) {
        val next = lineScanner.next()
        if (inQuote) {
          val checkQuoteFinished(middle, endOfQuote) = next
          if (endOfQuote != null) {
            quoteBuilder ++= middle
          } else {
            results += quoteBuilder.result
            position += 1
            inQuote = false
          }
        } else {
          val stripWhitespaceCheckQuoteState(completeValue, quoteBeginning) = next
          if (completeValue != null) {
            results += completeValue
            position += 1
          } else {
            quoteBuilder = new StringBuilder(quoteBeginning)
            inQuote = true
          }
        }
      }

      results
    }

    val file = new BufferedReader(new FileReader(location))
    var nextLine = file.readLine()
    var index = 0

    // if you want a row index, you don't have to explicitly specify the first column in columnsToParse
    val columnsToParse = rowIndex.fold(contentColumnsToParse) {
      indexPosition =>
      if (!contentColumnsToParse.isEmpty && !contentColumnsToParse.contains(indexPosition))
        indexPosition :: contentColumnsToParse
      else
        contentColumnsToParse
    }
    // first line might be the column index and not real values, also want to instantiate column cache
    val firstParsedLine = parseLine(nextLine, columnsToParse)
    val numberOfColumns = firstParsedLine.length
    val columnsSeq = 0 to numberOfColumns map (_.toString)
    val (colIndexArray, columns) =
      if (columnIndex)
        (firstParsedLine, ArrayBuffer[ArrayBuffer[String]](columnsSeq map { _ => ArrayBuffer[String]() }: _*))
      else
        (ArrayBuffer(columnsSeq: _*), ArrayBuffer[ArrayBuffer[String]]())

    while (nextLine != null) {
      val parsed = parseLine(nextLine, columnsToParse)
      while (index < numberOfColumns) {
        columns(index) += parsed(index)
        index += 1
      }

      nextLine = file.readLine()
      index = 0
    }
    file.close()

    Frame(
      Index(columns(0).toArray),
      colIndexArray.zip(
        columns.slice(1, columns.length) map {
          colArr => TypedColumn(Column.fromArray(colArr.toArray))
        }): _*)
  }
}
