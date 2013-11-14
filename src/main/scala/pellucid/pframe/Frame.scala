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

  def getRowHList[L <: HList: HListRowExtractor](row: Row): Option[L] = HListRowExtractor.extract(this, row)
}

trait HListRowExtractor[L <: HList] {
  def extract[Row,Col](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[L]
}

trait HListRowExtractorLow {
  implicit def hnilRowExtractor = new HListRowExtractor[HNil] {
    def extract[Row,Col](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[HNil] = cols match {
      case Nil => Some(HNil)
      case _ => None
    }
  }
}

object HListRowExtractor extends HListRowExtractorLow {
  implicit def hlistRowExtractor[H: Typeable: TypeTag, T <: HList](implicit tailExtractor: HListRowExtractor[T]) = new HListRowExtractor[H :: T] {
    def extract[Row,Col](frame: Frame[Row,Col], row: Row, colIndices: List[Col]): Option[H :: T] = for {
      idx <- colIndices.headOption
      tail <- tailExtractor.extract(frame, row, colIndices.tail)
      col <- frame.column[H](idx)
      value <- col(row).value
    } yield {
      value :: tail
    }
  }

  def extract[Row, Col, L <: HList](frame: Frame[Row,Col], row: Row)(implicit extractor: HListRowExtractor[L]): Option[L] =
    extractor.extract(frame, row, frame.colIndex.keys.toList)
}

object Frame {
  def apply[Row,Col: Order: ClassTag](rowIndex: Index[Row], colPairs: (Col,Column[_])*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    Frame(rowIndex, Index(colKeys: _*), cols.toArray)
  }
}
