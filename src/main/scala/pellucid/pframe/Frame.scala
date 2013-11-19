package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import spire.syntax.additiveMonoid._
import shapeless._
import shapeless.ops.function._

case class Frame[Row,Col](rowIndex: Index[Row], colIndex: Index[Col], cols: Array[UntypedColumn]) {
  def column[A: Typeable: TypeTag](k: Col): Series[Row,A] =
    Series(rowIndex, rawColumn(k) getOrElse Column.empty[A])

  def row[A: Typeable: TypeTag](k: Row)(implicit t: Typeable[Column[A]]): Option[Series[Col,A]] =
    rowIndex get k map { row =>
      Series(colIndex, Column.fromCells(cols map { _.cast[A].apply(row) }))
    }

  def columns: ColumnSelector[Row, Col, Variable] = ColumnSelector(this, colIndex.keys.toList)

  private def rawColumn[A: Typeable: TypeTag](k: Col): Option[Column[A]] = for {
    i <- colIndex.get(k)
  } yield cols(i).cast[A]
}

object Frame {
  def apply[Row,Col: Order: ClassTag](rowIndex: Index[Row], colPairs: (Col,UntypedColumn)*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    Frame(rowIndex, Index(colKeys: _*), cols.toArray)
  }
}

final case class ColumnSelector[Row, Col, Sz <: Size](frame: Frame[Row, Col], cols: List[Col]) {
  import Nat._

  type RowExtractorAux[A] = RowExtractor[A, Col, Sz]

  def apply(col: Col): ColumnSelector[Row, Col, Fixed[_1]] =
    new ColumnSelector[Row, Col, Fixed[_1]](frame, col :: Nil)

  def apply(col0: Col, col1: Col): ColumnSelector[Row, Col, Fixed[_2]] =
    new ColumnSelector[Row, Col, Fixed[_2]](frame, col0 :: col1 :: Nil)

  def apply(col0: Col, col1: Col, col2: Col): ColumnSelector[Row, Col, Fixed[_3]] =
    new ColumnSelector[Row, Col, Fixed[_3]](frame, col0 :: col1 :: col2 :: Nil)

  def getRowAs[A: RowExtractorAux](row: Row) = RowExtractor.extract(frame, row, cols)

  def as[A: RowExtractorAux]: Series[Row, A] = {
    val cells = frame.rowIndex.keys map { row =>
      RowExtractor.extract(frame, row, cols) map (Value(_)) getOrElse NA
    }
    Series(frame.rowIndex, Column.fromCells(cells.toVector))
  }

  def map[F, L <: HList, A](f: F)(implicit fntop: FnToProduct.Aux[F, L => A],
      e: RowExtractor[L, Col, Sz]): Series[Row, A] = {
    val fn = fntop(f)
    val cells = frame.rowIndex.keys map { row =>
      RowExtractor.extract(frame, row, cols) map fn map (Value(_)) getOrElse NA
    }
    Series(frame.rowIndex, Column.fromCells(cells.toVector))
  }

  def mapWithIndex[F, L <: HList, A](f: F)(implicit fntop: FnToProduct.Aux[F, (Row :: L) => A],
      e: RowExtractorAux[L]): Series[Row, A] = {
    val fn = fntop(f)
    val cells = frame.rowIndex.keys map { row =>
      RowExtractor.extract(frame, row, cols) map (row :: _) map fn map (Value(_)) getOrElse NA
    }
    Series(frame.rowIndex, Column.fromCells(cells.toVector))
  }

  def filter[F, L <: HList](f: F)(implicit fntop: FnToProduct.Aux[F, L => Boolean],
      e: RowExtractorAux[L]): Frame[Row,Col] = {
    val fn = fntop(f)
    val bits = new scala.collection.mutable.BitSet
    frame.rowIndex foreach { (key, row) =>
      bits(row) = RowExtractor.extract(frame, key, cols) map fn getOrElse false
    }
    val exists = bits.toImmutable
    Frame(frame.rowIndex, frame.colIndex, frame.cols map { _.mask(exists) })
  }
}
