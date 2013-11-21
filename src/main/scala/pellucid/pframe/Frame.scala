package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import shapeless._

trait Frame[Row, Col] {
  def rowIndex: Index[Row]
  def colIndex: Index[Col]

  private implicit def rowClassTag = rowIndex.classTag
  private implicit def rowOrder = rowIndex.order
  private implicit def colClassTag = colIndex.classTag
  private implicit def colOrder = colIndex.order

  def getColumns: Seq[(Col, UntypedColumn)]

  def column[A: Typeable: TypeTag](col: Col): Series[Row,A]
  def row[A: Typeable: TypeTag](row: Row): Series[Col,A]

  def columns: ColumnSelector[Row, Col] = ColumnSelector(this)

  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1]
  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col]

  def join(that: Frame[Row, Col])(join: Join): Frame[Row, Col] = {
    // TODO: This should use simpler things, like:
    //   this.reindex(lIndex).withRowIndex(newRowIndex) ++
    //   that.reindex(rIndex).withRowIndex(newRowIndex)

    val joiner = Joiner[Row](join)(rowIndex.classTag)
    val (keys, lIndex, rIndex) = Index.cogroup(this.rowIndex, that.rowIndex)(joiner).result()
    val newRowIndex = Index.ordered(keys)
    val cols0 = this.getColumns map { case (key, col) =>
      (key, col.setNA(Joiner.Skip).reindex(lIndex))
    }
    val cols1 = that.getColumns map { case (key, col) =>
      (key, col.setNA(Joiner.Skip).reindex(rIndex))
    }
    val (newColIndex, cols) = (cols0 ++ cols1).unzip
    ColOrientedFrame(newRowIndex, Index(newColIndex: _*), cols.toArray)
  }
}

case class ColOrientedFrame[Row, Col](
      rowIndex: Index[Row],
      colIndex: Index[Col],
      cols: Array[UntypedColumn])
    extends Frame[Row, Col] {

  def getColumns: Seq[(Col, UntypedColumn)] = colIndex.map({ case (key, i) =>
    (key, cols(i))
  })(collection.breakOut)

  def column[A: Typeable: TypeTag](k: Col): Series[Row,A] =
    Series(rowIndex, rawColumn(k) getOrElse Column.empty[A])

  def row[A: Typeable: TypeTag](k: Row): Series[Col,A] =
    Series(colIndex, rowIndex get k map { row =>
      Column.fromCells(cols map { _.cast[A].apply(row) })
    } getOrElse Column.empty[A])

  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1] =
    ColOrientedFrame(rowIndex, ci, cols)

  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col] =
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
