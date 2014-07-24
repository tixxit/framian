package framian
package csv

import spire.syntax.monoid._

sealed abstract class CsvCell {
  def render(format: CsvFormat): String
}

object CsvCell {
  case class Data(value: String) extends CsvCell {
    def render(format: CsvFormat): String = format.render(value)
    override def toString: String = value
  }
  case object Empty extends CsvCell {
    def render(format: CsvFormat): String = format.empty
    override def toString: String = "-"
  }
  case object Invalid extends CsvCell {
    def render(format: CsvFormat): String = format.invalid
    override def toString: String = "<error>"
  }

  def fromNonValue(nonValue: NonValue): CsvCell = nonValue match {
    case NA => Empty
    case NM => Invalid
  }

  implicit object CsvCellColumnTyper extends ColumnTyper[CsvCell] {
    def cast(col: TypedColumn[_]): Column[CsvCell] = {
      val num = col.cast[BigDecimal] map (n => Data(n.toString): CsvCell)
      val text = col.cast[String] map (Data(_): CsvCell)
      val any = col.cast[Any] map (any => Data(any.toString): CsvCell)
      num |+| text |+| any
    }
  }
}
