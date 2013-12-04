package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import spire.implicits._
import spire.compat._
import shapeless.{HList, Typeable, Poly2, LUBConstraint}
import shapeless.ops.hlist.{LeftFolder, Length}
import shapeless.syntax.typeable._
import shapeless.syntax._

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
    reindexColumns: (Array[Row], Array[Int], Array[Int]) => T => Seq[(Col, UntypedColumn)]
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
    val cols1 = reindexColumns(keys, lIndex, rIndex)(that)

    val (newColIndex, cols) = (cols0 ++ cols1).unzip
    ColOrientedFrame(newRowIndex, Index(newColIndex.toArray), cols.toArray)
  }

  def join(that: Frame[Row, Col])(join: Join): Frame[Row, Col] =
    genericJoin[Frame[Row, Col]](
      { frame: Frame[Row, Col] => frame.rowIndex },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => frame: Frame[Row, Col] =>
        frame.getColumns map { case (key, col) =>
          (key, col.setNA(Joiner.Skip).reindex(rIndex))
        }
      }
    )(that)(join)

  def join[T: TypeTag](that: Series[Row, T], columnKey: Col)(join: Join): Frame[Row, Col] =
    genericJoin[Series[Row, T]](
      { series: Series[Row, T] => series.index },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => series: Series[Row, T] =>
        Seq((columnKey, TypedColumn(series.column.setNA(Joiner.Skip).reindex(rIndex))))
      })(that)(join)

  import LUBConstraint.<<:
  import Frame.joinSeries
  def join[TSeries <: HList : <<:[Series[Row, _]]#λ]
          (them: TSeries, columnKeys: Seq[Col] = Seq())
          (join: Join)
          (implicit
             tf1: LeftFolder.Aux[TSeries, (List[Col], Frame[Row, Col]), joinSeries.type, (List[Col], Frame[Row, Col])],
             tpe: Typeable[Index[Col]]
          ): Frame[Row, Col] = {
    val columnIndices: Seq[Col] =
      if (columnKeys.isEmpty)
        (colIndex.cast[Index[Int]] match {
           case Some(_) if colIndex.isEmpty => 0 to (them.runtimeLength - 1)
           case Some(index) =>
             val colMax = index.max._1
             (colMax + 1) to (colMax + them.runtimeLength)
           case _ => throw new Exception("Cannot create default column index values if column type is not numeric.")
         }).toSeq.asInstanceOf[Seq[Col]]
      else columnKeys

    them.foldLeft(columnIndices.toList, this)(joinSeries)._2
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

  object joinSeries extends Poly2 {
    implicit def caseT[T: TypeTag, Row: Order: ClassTag, Col: Order: ClassTag] =
      at[(List[Col], Frame[Row, Col]), Series[Row, T]] {
        case ((columnIndex :: columnIndices, frame), series) =>
          (columnIndices, frame.join(series, columnIndex)(Join.Outer))
      }
  }

  def apply[Row,Col: Order: ClassTag](rowIndex: Index[Row], colPairs: (Col,UntypedColumn)*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    ColOrientedFrame(rowIndex, Index(colKeys.toArray), cols.toArray)
  }

  import LUBConstraint.<<:
  def apply[Row: Order: ClassTag, Col: Order: ClassTag, TSeries <: HList : <<:[Series[Row, _]]#λ]
           (colIndex: Index[Col], colSeries: TSeries)
           (implicit
              tf: LeftFolder.Aux[TSeries,(List[Col], Frame[Row,Col]), joinSeries.type, (List[Col], Frame[Row,Col])]
           ): Frame[Row,Col] = {
    val frame: Frame[Row, Col] = ColOrientedFrame[Row, Col](Index[Row](), Index[Col](), Array())
    frame.join(colSeries, colIndex.keys.toSeq)(Join.Outer)
  }

  def apply[Row: Order: ClassTag, Col: Order: ClassTag, TSeries <: HList : <<:[Series[Row, _]]#λ]
           (colSeries: TSeries)
           (implicit
              tf: LeftFolder.Aux[TSeries,(List[Col], Frame[Row,Col]), joinSeries.type, (List[Col], Frame[Row,Col])]
           ): Frame[Row,Col] =
    apply(Index[Col](), colSeries)

  def apply()
}
