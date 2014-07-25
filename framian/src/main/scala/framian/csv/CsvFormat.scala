package framian.csv

import java.io.{ Reader, PushbackReader }
import java.util.regex.Pattern

sealed abstract class CsvRowDelim(val value: String, val alternate: Option[String] = None)
object CsvRowDelim {
  case object Unix extends CsvRowDelim("\n")
  case object Windows extends CsvRowDelim("\r\n")
  case object Both extends CsvRowDelim("\n", Some("\r\n"))
}

sealed trait CsvFormatStrategy

trait GuessCsvFormat extends CsvFormatStrategy {

  /**
   * Makes a guess at the format of the CSV accessed by `reader`. This returns
   * the format, as well as the a new pushback reader to be used in place of
   * `reader`. The original reader will have some data read out of it. The
   * returned reader will contain all the original reader's data.
   */
  def apply(reader: Reader): (CsvFormat, Reader) = {
    val reader0 = new PushbackReader(reader, Csv.BufferSize)
    val buffer = new Array[Char](Csv.BufferSize)
    val len = reader0.read(buffer)
    reader0.unread(buffer, 0, len)

    val chunk = new String(buffer, 0, len)
    val format = apply(chunk)
    (format, reader0)
  }

  /**
   * Given the first part of a CSV file, return a guess at the format.
   */
  def apply(str: String): CsvFormat
}

case class CsvFormat(
  separator: String,
  quote: String = "\"",
  quoteEscape: String = "\"",
  empty: String = "",
  invalid: String = "",
  header: Boolean = false,
  rowDelim: CsvRowDelim = CsvRowDelim.Both
) extends CsvFormatStrategy {
  val escapedQuote = quoteEscape + quote

  override def toString: String =
    s"""CsvFormat(separator = "$separator", quote = "$quote", quoteEscape = "$quoteEscape", empty = "$empty", invalid = "$invalid", header = $header, rowDelim = $rowDelim)"""

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

  val Guess = Partial()

  case class Partial(
      separator: Option[String] = None,
      quote: Option[String] = None,
      quoteEscape: Option[String] = None,
      empty: Option[String] = None,
      invalid: Option[String] = None,
      header: Option[Boolean] = None,
      rowDelim: Option[CsvRowDelim] = None
    ) extends GuessCsvFormat {

    /**
     * Performs a very naive guess of the CsvFormat. This uses weighted
     * frequencies of occurences of common separators, row-delimiters, quotes,
     * quote escapes, etc. and simply selects the max for each.
     */
    def apply(str: String): CsvFormat = {
      def count(ndl: String): Int = {
        def check(i: Int, j: Int = 0): Boolean =
          if (j >= ndl.length) true
          else if (i < str.length && str.charAt(i) == ndl.charAt(j)) check(i + 1, j + 1)
          else false

        def loop(i: Int, cnt: Int): Int =
          if (i < str.length) {
            loop(i + 1, if (check(i)) cnt + 1 else cnt)
          } else cnt

        loop(0, 0)
      }

      def choose(weightedOptions: (String, Double)*)(f: String => Int): String = {
        val weights = Map(weightedOptions: _*)
        val (best, weight) = weights.maxBy { case (c, w) => w * f(c) }
        if (weight > 0) best else weights.maxBy(_._2)._1
      }

      val rowDelim0 = rowDelim.getOrElse {
        val windCnt = count("\r\n")
        val unixCnt = count("\n")

        if ((windCnt < 4 * unixCnt) && (unixCnt < 4 * windCnt)) CsvRowDelim.Both
        else if (windCnt < 4 * unixCnt) CsvRowDelim.Unix
        else CsvRowDelim.Windows
      }
      val separator0 = separator.getOrElse {
        choose(","  -> 1.0, "\t" -> 3.0, ";"  -> 2.0, "|"  -> 3.0)(count)
      }
      val quote0 = quote.getOrElse(choose("\"" -> 1.2, "\'" -> 1)(count))
      val quoteEscape0 = choose(s"$quote0$quote0" -> 1.1, s"\\$quote0" -> 1)(count).dropRight(quote0.length)

      val cells = for {
        row0 <- str.split(Pattern.quote(rowDelim0.value))
        row <- rowDelim0.alternate.fold(Array(row0)) { alt =>
            row0.split(Pattern.quote(alt))
          }
        cell <- row.split(Pattern.quote(separator0))
      } yield cell
      def matches(value: String): Int = cells.filter(_ == value).size
      val empty0 = empty.getOrElse {
        choose("" -> 3, "?" -> 2, "-" -> 2, "N/A" -> 1, "NA" -> 1)(matches)
      }
      val invalid0 = invalid.getOrElse {
        if (matches("N/M") > 1) "N/M" else empty0
      }

      val header0 = header.getOrElse(hasHeader(str, rowDelim0.value, separator0, quote0))

      CsvFormat(separator0, quote0, quoteEscape0, empty0, invalid0, header0, rowDelim0)
    }

    def hasHeader(chunk: String, rowDelim: String, separator: String, quote: String): Boolean = {
      import spire.std.map._
      import spire.std.double._
      import spire.syntax.all._

      def mkVec(s: String): Map[Char, Double] =
        s.groupBy(c => c).map { case (k, v) => k -> v.length.toDouble }.normalize

      def similarity[K](x: Map[K, Double], y: Map[K, Double]): Double = (x dot y) / (x.norm * y.norm)

      val headerEnd = chunk.indexOf(rowDelim)
      if (headerEnd > 0) {
        val (hdr, rows) = chunk.replace(separator, "").replace(quote, "").splitAt(headerEnd)
        similarity(mkVec(hdr), mkVec(rows)) < 0.5
      } else {
        false
      }
    }
  }
}
