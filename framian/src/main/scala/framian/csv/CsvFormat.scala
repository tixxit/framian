package framian.csv

import java.io.{ Reader, PushbackReader }

sealed abstract class CsvRowDelim(val value: String, val alternate: Option[String] = None)
object CsvRowDelim {
  case object Unix extends CsvRowDelim("\n")
  case object Windows extends CsvRowDelim("\r\n")
  case object Both extends CsvRowDelim("\n", Some("\r\n"))
}

case class CsvFormat(
  separator: String,
  quote: String = "\"",
  quoteEscape: String = "\"",
  empty: String = "NA",
  invalid: String = "NM",
  header: Boolean = true,
  rowDelim: CsvRowDelim = CsvRowDelim.Both
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
  val BufferSize = 32 * 1024

  val CSV = CsvFormat(",")
  val TSV = CsvFormat("\t")

  /**
   * Makes a guess at the format of the CSV accessed by `reader`. This returns
   * the format, as well as the a new pushback reader to be used in place of
   * `reader`. The original reader will have some data read out of it. The
   * returned reader will contain all the original reader's data.
   */
  def guess(reader: Reader): (CsvFormat, Reader) = {
    val reader0 = new PushbackReader(reader, BufferSize)
    val buffer = new Array[Char](BufferSize)
    val len = reader0.read(buffer)
    reader0.unread(buffer, 0, len)

    val chunk = new String(buffer, 0, len)
    val format = guess(chunk)
    (format, reader0)
  }

  /**
   * Performs a very naive guess of the CsvFormat. This uses weighted
   * frequencies of occurences of common separators, row-delimiters, quotes,
   * quote escapes, etc. and simply selects the max for each.
   */
  def guess(str: String): CsvFormat = {
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

    def choose(weightedOptions: (String, Double)*): String = {
      val weights = Map(weightedOptions: _*)
      val (best, weight) = weights.maxBy { case (c, w) => w * count(c) }
      if (weight > 0) best else weights.maxBy(_._2)._1
    }

    val windCnt = count("\r\n")
    val unixCnt = count("\n")
    val rowDelim =
      if ((windCnt < 4 * unixCnt) && (unixCnt < 4 * windCnt)) CsvRowDelim.Both
      else if (windCnt < 4 * unixCnt) CsvRowDelim.Unix
      else CsvRowDelim.Windows
    val separator = choose(
        ","  -> 1.0,
        "\t" -> 3.0,
        ";"  -> 2.0,
        "|"  -> 3.0
      )
    val quote = choose("\"" -> 1.2, "\'" -> 1)
    val quoteEscape = choose(s"$quote$quote" -> 1.1, s"\\$quote" -> 1).dropRight(quote.length)
    val empty = choose("-" -> 1.5, "N/A" -> 2, "NA" -> 1)
    val invalid = choose("N/M" -> 2, "NaN" -> 1)

    CsvFormat(separator, quote, quoteEscape, empty, invalid, true, rowDelim)
  }
}
