package framian.csv

case class CsvError(message: String, rowStart: Long, pos: Long, context: String, row: Long, col: Long) {
  def description: String = {
    val msg = s"Error parsing CSV row: $message"
    val prefix = s"Row $row: "
    val padLength = col.toInt - 1 + prefix.length
    val pointer = (" " * padLength) + "^"

    s"$msg\n\n$prefix$context\n$pointer"
  }
}
