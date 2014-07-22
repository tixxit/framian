package framian.csv

case class CsvError(message: String, pos: Long, context: String, row: Long, col: Long)
