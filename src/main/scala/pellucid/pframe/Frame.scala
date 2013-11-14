package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import shapeless._

case class Frame[Row,Col](rowIndex: Index[Row], colIndex: Index[Col], cols: Array[Column[_]]) {
  def rawColumn[A: Typeable: TypeTag](k: Col): Option[Column[A]] = for {
    i <- colIndex.get(k)
  } yield Column.cast[A](cols(i))

  def column[A: Typeable: TypeTag](k: Col): Option[Series[Row,A]] =
    rawColumn(k) map (Series(rowIndex, _))

  def row[A: Typeable: TypeTag](k: Row)(implicit t: Typeable[Column[A]]): Option[Series[Col,A]] =
    rowIndex get k map { row =>
      Series(colIndex, CellColumn(cols map { col => Column.cast[A](col).apply(row) }))
    }

  def getRowRecord[L: RowExtractor](row: Row): Option[L] = RowExtractor.extract(this, row)
}

object Frame {
  def apply[Row,Col: Order: ClassTag](rowIndex: Index[Row], colPairs: (Col,Column[_])*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    Frame(rowIndex, Index(colKeys: _*), cols.toArray)
  }
}

trait RowExtractor[A] {
  def extract[Row,Col](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[A]
}

trait RowExtractorLow0 {
  implicit def generic[A,B](implicit generic: Generic.Aux[A,B], extractor: RowExtractor[B]) = new RowExtractor[A] {
    def extract[Row,Col](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[A] =
      extractor.extract(frame, row, cols) map (generic.from(_))
  }
}

trait RowExtractorLow1 extends RowExtractorLow0 {
  implicit def hnilRowExtractor = new RowExtractor[HNil] {
    def extract[Row,Col](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[HNil] = cols match {
      case Nil => Some(HNil)
      case _ => None
    }
  }
}

object RowExtractor extends RowExtractorLow1 {
  implicit def hlistRowExtractor[H: Typeable: TypeTag, T <: HList](implicit tailExtractor: RowExtractor[T]) = new RowExtractor[H :: T] {
    def extract[Row,Col](frame: Frame[Row,Col], row: Row, colIndices: List[Col]): Option[H :: T] = for {
      idx <- colIndices.headOption
      tail <- tailExtractor.extract(frame, row, colIndices.tail)
      col <- frame.column[H](idx)
      value <- col(row).value
    } yield {
      value :: tail
    }
  }

  def extract[Row, Col, A](frame: Frame[Row,Col], row: Row)(implicit extractor: RowExtractor[A]): Option[A] =
    extractor.extract(frame, row, frame.colIndex.keys.toList)
}

