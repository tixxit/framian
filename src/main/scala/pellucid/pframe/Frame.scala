package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import shapeless._

trait Frame[Row, Col] {
  def rowIndex: Index[Row]
  def colIndex: Index[Col]

  def column[A: Typeable: TypeTag](col: Col): Series[Row,A]
  def row[A: Typeable: TypeTag](row: Row): Series[Col,A]

  def columns: ColumnSelector[Row, Col, Variable] = ColumnSelector(this, colIndex.keys.toList)

  def withColIndex(ci: Index[Col]): Frame[Row, Col]
  def withRowIndex(ri: Index[Row]): Frame[Row, Col]
}

case class ColOrientedFrame[Row, Col](
      rowIndex: Index[Row],
      colIndex: Index[Col],
      cols: Array[UntypedColumn])
    extends Frame[Row, Col] {

  def column[A: Typeable: TypeTag](k: Col): Series[Row,A] =
    Series(rowIndex, rawColumn(k) getOrElse Column.empty[A])

  def row[A: Typeable: TypeTag](k: Row): Series[Col,A] =
    Series(colIndex, rowIndex get k map { row =>
      Column.fromCells(cols map { _.cast[A].apply(row) })
    } getOrElse Column.empty[A])

  def withColIndex(ci: Index[Col]): Frame[Row, Col] =
    ColOrientedFrame(rowIndex, ci, cols)

  def withRowIndex(ri: Index[Row]): Frame[Row, Col] =
    ColOrientedFrame(ri, colIndex, cols)

  private def rawColumn[A: Typeable: TypeTag](k: Col): Option[Column[A]] = for {
    i <- colIndex.get(k)
  } yield cols(i).cast[A]
}

object Frame {
  def apply[Row,Col: Order: ClassTag](rowIndex: Index[Row], colPairs: (Col,UntypedColumn)*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    ColOrientedFrame(rowIndex, Index(colKeys: _*), cols.toArray)
  }
}
