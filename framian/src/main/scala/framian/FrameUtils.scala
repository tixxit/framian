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

package framian

import java.util.Scanner
import java.io._

import scala.collection.mutable.ArrayBuffer

import scala.io.Source
import spire.math.Number
import spire.implicits._

import framian.column._

package object utilities {

  /** Load a frame from CSV.
    *
    * @param delimiter how values in csv are delimited (tabs, commas, etc). defaults, per name of method, to comma.
    * @param columnIndex whether or not the first row is expected to indicate the column index of the frame
    * @param rowIndex whether or not the first column is expected to express the row index of the frame
    */
  def loadFrameFromCSV(
    csvFile: File,
    delimiter: String = ",",
    quote: String = "\"",
    rowIndex: Int = -1,
    columnIndex: Boolean = false,
    columns: List[Int] = List()
    ) = {
    // sometimes you just want the bulk of a state-machine compiled for you...
    val stripWhitespaceCheckQuoteState =
      s"(?:[\\s]*($quote[^$quote]*$quote|[^\\s$quote]*(?:[\\s]*[^\\s$quote]*)*)[\\s]*)|[\\s]*($quote[^$quote]*)".r
    val checkQuoteFinished = s"([^$quote]*)|([^$quote]*$quote)[\\s]*".r

    val stripQuotes = s"$quote?([^$quote]*)$quote?".r

    val file = new BufferedReader(new FileReader(csvFile))

    var nextLine = file.readLine()

    // for now we're just making sure that the delimiter produces more than one column...
    // otherwise, assuming misconfigured and not even trying to parse the file (ie comma on tsv)
    assert(nextLine.split(delimiter).length > 1)

    // assuming sorted list of desired columns...
    def parseLine(line: String, columns: List[Int] = List()): ArrayBuffer[String] = {
      val lineScanner = new Scanner(line).useDelimiter(delimiter)
      val results = ArrayBuffer[String]()
      val columnsNotProvided = columns.isEmpty

      var inQuote = false
      var position = 0
      var quoteBuilder: StringBuilder = null
      var remainingColumns = columns

      def takeColumnIfRequested(value: String) = {
        if (columnsNotProvided || remainingColumns.head == position) {
          if (!columnsNotProvided) remainingColumns = remainingColumns.tail
          val stripQuotes(cleanedValue) = value
          results += cleanedValue
        }
        position += 1
      }

      while (lineScanner.hasNext && (columnsNotProvided || !remainingColumns.isEmpty)) {
        val next = lineScanner.next()
        if (inQuote) {
          val checkQuoteFinished(middle, endOfQuote) = next
          // either we're in the middle of a quote in which case add the middle
          // to builder and move to the next segment of the lineScanner
          if (middle != null)
            quoteBuilder ++= delimiter + middle
          // or we're at the end and need to add final value to quote builder and take column if needed
          else {
            quoteBuilder ++= delimiter + endOfQuote
            takeColumnIfRequested(quoteBuilder.result)
            inQuote = false
          }
        } else {
          val stripWhitespaceCheckQuoteState(completeValue, quoteBeginning) = next
          if (completeValue != null)
            takeColumnIfRequested(completeValue)
          else {
            quoteBuilder = new StringBuilder(quoteBeginning)
            inQuote = true
          }
        }
      }

      results
    }

    // if you want a row index, you don't have to explicitly specify the first column in columnsToParse
    val columnsToParse =
      if (rowIndex < 0)
        columns
      else {
        if (!columns.isEmpty && !columns.contains(rowIndex))
          (rowIndex :: columns).sorted
        else
          columns
      }

    // first line might be the column index and not real values, also want to instantiate column cache
    val firstLine = parseLine(nextLine, columnsToParse)
    val numberOfColumns = firstLine.length
    // we either want to pull out that first row as the column index or produce a default integer index
    val columnsSeq = 0 to (numberOfColumns - 1) map (_.toString)
    val parsedColumns = ArrayBuffer[ArrayBuffer[String]](columnsSeq map { _ => ArrayBuffer[String]() }: _*)

    // need to make sure that we parse the first line if it isn't a column index.
    val colIndexArray =
      if (columnIndex) {
        nextLine = file.readLine()
        firstLine
      } else
        ArrayBuffer(columnsSeq: _*)

    var index = 0
    while (nextLine != null) {
      val parsed = parseLine(nextLine, columnsToParse)
      while (index < numberOfColumns) {
        parsedColumns(index) += parsed(index)
        index += 1
      }

      index = 0
      nextLine = file.readLine()
    }
    file.close()

    // either make a row index now that we know how many rows or grab the user specified row index column
    // also, if there's a row index we need to drop first value in column index as un-needed.
    val (rowIndexValues, valueColumns, valueColumnIndex) =
      if (rowIndex < 0)
        ((0 to parsedColumns(0).length - 1) map (_.toString), parsedColumns, colIndexArray)
      else {
        val rowIndexPosition = if (columnsToParse.isEmpty) rowIndex else columnsToParse.indexOf(rowIndex)
        val (colIndexLeft, colIndexRight) = colIndexArray.splitAt(rowIndexPosition)
        val (colsLeft, colsRight) = parsedColumns.splitAt(rowIndexPosition)
        (parsedColumns(rowIndexPosition),
         colsLeft ++ colsRight.drop(1),
         // if column index, make sure row index name not a part of it
         if (columnIndex) colIndexLeft ++ colIndexRight.drop(1)
         // otherwise, make sure we get one fewer column index value than expected
         else colIndexArray.slice(0, parsedColumns.length - 1))
      }

    ColOrientedFrame(
      Index(rowIndexValues.toArray),
      Series(valueColumnIndex.zip(
        valueColumns map {
          colArr => TypedColumn(Column.dense(colArr.toArray))
        }): _*))
  }
}
