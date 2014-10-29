package framian.csv

sealed trait ParserState {
  import ParserState._

  def rowStart: Long
  def readFrom: Long
  def input: Input

  def withInput(input0: Input): ParserState = this match {
    case ContinueRow(_, _, partial, _) => ContinueRow(rowStart, readFrom, partial, input0)
    case SkipRow(_, _, _) => SkipRow(rowStart, readFrom, input0)
    case ParseRow(_, _, _) => ParseRow(rowStart, readFrom, input0)
  }

  def mapInput(f: Input => Input): ParserState = withInput(f(input))
}

object ParserState {
  case class ContinueRow(rowStart: Long, readFrom: Long, partial: Vector[CsvCell], input: Input) extends ParserState
  case class SkipRow(rowStart: Long, readFrom: Long, input: Input) extends ParserState
  case class ParseRow(rowStart: Long, readFrom: Long, input: Input) extends ParserState
}
