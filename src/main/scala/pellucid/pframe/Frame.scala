package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import spire.syntax.additiveMonoid._
import shapeless._
import shapeless.ops.function._

case class Frame[Row,Col](rowIndex: Index[Row], colIndex: Index[Col], cols: Array[Column[_]]) {
  def rawColumn[A: Typeable: TypeTag](k: Col): Option[Column[A]] = for {
    i <- colIndex.get(k)
  } yield Column.cast[A](cols(i))

  def column[A: Typeable: TypeTag](k: Col): Series[Row,A] =
    Series(rowIndex, rawColumn(k) getOrElse Column.empty[A])

  def columns[A: Typeable: TypeTag](col: Col): Series[Row,A] = column(col)

  def columns[A: RowExtractor: TypeTag](col0: Col, col1: Col, rest: Col*): Series[Row,A] = {
    val cols = col0 :: col1 :: rest.toList
    val cells = rowIndex.keys map { row =>
      RowExtractor.extract(this, row, cols) map (Value(_)) getOrElse NA
    }
    Series(rowIndex, CellColumn(cells.toVector))
  }

  def row[A: Typeable: TypeTag](k: Row)(implicit t: Typeable[Column[A]]): Option[Series[Col,A]] =
    rowIndex get k map { row =>
      Series(colIndex, CellColumn(cols map { col => Column.cast[A](col).apply(row) }))
    }

  def getRecord[A: RowExtractor](row: Row): Option[A] = RowExtractor.extract(this, row)

  def sum[A: Typeable: TypeTag](col: Col)(implicit A: AdditiveMonoid[A]): A =
    columns[A](col).sum

  def sum[A: RowExtractor: TypeTag](col0: Col, col1: Col, rest: Col*)(implicit A: AdditiveMonoid[A]): A =
    columns[A](col0, col1, rest: _*).sum

  def mapRows[F, L <: HList, B](cols: Col*)(f: F)(implicit fntop: FnToProduct.Aux[F, L => B], ex: RowExtractor[L], ttB: TypeTag[B]): Series[Row,B] = {
    val fn = fntop(f)
    val cells = rowIndex.keys map { row =>
      RowExtractor.extract(this, row, cols.toList) map fn map (Value(_)) getOrElse NA
    }
    Series(rowIndex, CellColumn(cells.toVector))
  }

  def mapRows[F, L <: HList, B](f: F)(implicit fntop: FnToProduct.Aux[F, L => B], ex: RowExtractor[L], ttB: TypeTag[B]): Series[Row,B] =
    mapRows(colIndex.keys: _*)(f)

  def mapRowsWithIndex[F, L <: HList, B](cols: Col*)(f: F)(implicit fntop: FnToProduct.Aux[F, (Row :: L) => B], ex: RowExtractor[L], ttB: TypeTag[B]): Series[Row,B] = {
    val fn = fntop(f)
    val cells = rowIndex.keys map { row =>
      RowExtractor.extract(this, row, cols.toList) map (row :: _) map fn map (Value(_)) getOrElse NA
    }
    Series(rowIndex, CellColumn(cells.toVector))
  }

  def filter[F, L <: HList](colKeys: Col*)(f: F)(implicit fntop: FnToProduct.Aux[F, L => Boolean], ex: RowExtractor[L]): Frame[Row,Col] = {
    val fn = fntop(f)
    val bits = new scala.collection.mutable.BitSet
    val cells = (rowIndex.keys zip rowIndex.indices) map { case (key, row) =>
      bits(row) = RowExtractor.extract(this, key, colKeys.toList) map fn getOrElse false
    }
    val exists = bits.toImmutable
    Frame(rowIndex, colIndex, cols map { col => Column.filtered(bits.toImmutable, col) })
  }
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
      value <- frame.column[H](idx).apply(row).value
    } yield {
      value :: tail
    }
  }

  def extract[Row, Col, A](frame: Frame[Row,Col], row: Row)(implicit extractor: RowExtractor[A]): Option[A] =
    extractor.extract(frame, row, frame.colIndex.keys.toList)

  def extract[Row, Col, A](frame: Frame[Row,Col], row: Row, cols: List[Col])(implicit extractor: RowExtractor[A]): Option[A] =
    extractor.extract(frame, row, cols)
}
