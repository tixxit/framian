package framian.csv

sealed trait ParserState {
  import ParserState._

  def input: Input

  def withInput(input0: Input): ParserState = this match {
    case ContinueRow(partial, _) => ContinueRow(partial, input0)
    case SkipRow(_) => SkipRow(input0)
    case ParseRow(_) => ParseRow(input0)
  }

  def mapInput(f: Input => Input): ParserState = withInput(f(input))
}

object ParserState {
  case class ContinueRow(partial: Vector[CsvCell], input: Input) extends ParserState
  case class SkipRow(input: Input) extends ParserState
  case class ParseRow(input: Input) extends ParserState
}
