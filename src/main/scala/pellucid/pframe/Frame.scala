package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import spire.implicits._
import shapeless._
import shapeless.ops.hlist._

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

  private def genericJoin[T](
    getIndex: T => Index[Row],
    reindexColumns: Array[Int] => T => Seq[(Col, UntypedColumn)]
  )(that: T)(join: Join): Frame[Row, Col] = {
    // TODO: This should use simpler things, like:
    //   this.reindex(lIndex).withRowIndex(newRowIndex) ++
    //   that.reindex(rIndex).withRowIndex(newRowIndex)
    val joiner = Joiner[Row](join)(rowIndex.classTag)
    val (keys, lIndex, rIndex) = Index.cogroup(this.rowIndex, getIndex(that))(joiner).result()
    val newRowIndex = Index.ordered(keys)
    val cols0 = this.getColumns map { case (key, col) =>
      (key, col.setNA(Joiner.Skip).reindex(lIndex))
    }
    val cols1 = reindexColumns(rIndex)(that)

    val (newColIndex, cols) = (cols0 ++ cols1).unzip
    ColOrientedFrame(newRowIndex, Index(newColIndex.toArray), cols.toArray)
  }

  def join(that: Frame[Row, Col])(join: Join): Frame[Row, Col] =
    genericJoin[Frame[Row, Col]](
      { frame: Frame[Row, Col] => frame.rowIndex },
      { rIndex: Array[Int] => frame: Frame[Row, Col] =>
        frame.getColumns map { case (key, col) =>
          (key, col.setNA(Joiner.Skip).reindex(rIndex))
        }
      }
    )(that)(join)

  //def f[FooList <: HList : <<:[Foo[_]]#λ](foos: FooList)(implicit map: shapeless.ops.hlist.Mapper[fooFunc.type, FooList]) =
  //         foo map fooFunc

  import LUBConstraint.<<:
  def join[TSeries <: HList : <<:[Series[Row, _]]#λ]
          (them: TSeries, columnKeys: Seq[Col] = Seq())
          (join: Join): Frame[Row, Col] = {
    import Zipper._
    import Nat._

    val joiner = Joiner[Row](join)(rowIndex.classTag)
    object getIndex extends Poly2 {
      implicit def caseT[T] = at[Index[Row], Series[Row, T]] {
        (acc, series) => Index.cogroup(acc, series.index)(joiner).result()
      }
    }

    //implicit val map: Mapper[getIndex.type, TSeries] = implicitly[Mapper[getIndex.type, TSeries]]
    implicit lazy val folder: LeftFolder[TSeries, Index[Row], getIndex.type] =
      implicitly[LeftFolder[TSeries, Index[Row], getIndex.type]]

    genericJoin[TSeries](
      { series: TSeries => series.foldLeft(Index[Row]())(getIndex).asInstanceOf[Index[Row]] },
      { rIndex: Array[Int] => series: TSeries =>

        object makeColumns extends Poly2 {
          val columnIndex =
            if (columnKeys.isEmpty) 0 to columnKeys.length toArray else columnKeys.toArray

          implicit def caseT[T: TypeTag] = at[(Int, List[(Col, UntypedColumn)]), Series[Row, T]] {
            case ((i, columns), currentSeries) =>
              (i + 1,
               (columnIndex(i),
                TypedColumn(
                  currentSeries.column.setNA(Joiner.Skip).reindex(rIndex))
               ) :: columns)
          }
        }
        implicit lazy val folder: LeftFolder[TSeries, (Int, List[(Col, UntypedColumn)]), makeColumns.type] =
          implicitly[LeftFolder[TSeries, (Int, List[(Col, UntypedColumn)]), makeColumns.type]]

        series.foldLeft((0, List[(Col, UntypedColumn)]()))(makeColumns).asInstanceOf[(Int, List[(Col, UntypedColumn)])]._2
      })(them)(join)
  }

  def join[T: TypeTag](that: Series[Row, T], columnKey: Col)(join: Join): Frame[Row, Col] =
    genericJoin[Series[Row, T]](
      { series: Series[Row, T] => series.index },
      { rIndex: Array[Int] => series: Series[Row, T] =>
        Seq((columnKey, TypedColumn(series.column.setNA(Joiner.Skip).reindex(rIndex))))
      })(that)(join)

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
    ColOrientedFrame(rowIndex, Index(colKeys.toArray), cols.toArray)
  }

  /*def apply[Row,Col: Order: ClassTag](colIndex: Index[Col], colSeries: Series[Row, ]*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    ColOrientedFrame(rowIndex, Index(colKeys.toArray), cols.toArray)
  }*/



  def apply[Col: Order: ClassTag](colPairs: (Col,UntypedColumn)*): Frame[Int,Col] = {
    val (colKeys, cols) = colPairs.unzip
    ColOrientedFrame(Index(0 to (colKeys.length - 1) toArray), Index(colKeys.toArray), cols.toArray)
  }
}
