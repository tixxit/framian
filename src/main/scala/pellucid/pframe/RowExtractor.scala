package pellucid
package pframe

import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import spire.syntax.additiveMonoid._
import shapeless._
import shapeless.ops.function._

trait RowExtractor[A, Col, RowSize <: Size] {
  def extract[Row](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[A]
}

trait RowExtractorLow0 {
  implicit def variableExtractorIsFixed[A, Col, N <: Nat](implicit e: RowExtractor[A, Col, Variable]) =
    new RowExtractor[A, Col, Fixed[N]] {
      def extract[Row](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[A] =
        e.extract(frame, row, cols)
    }
}

trait RowExtractorLow1 {
  implicit def generic[A, B, Col, S <: Size](implicit generic: Generic.Aux[A, B],
      extractor: RowExtractor[B, Col, S]) = new RowExtractor[A,Col,S] {
    def extract[Row](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[A] =
      extractor.extract(frame, row, cols) map (generic.from(_))
  }
}

trait RowExtractorLow2 extends RowExtractorLow1 {
  implicit def simpleRowExtractor[A: Typeable: TypeTag, Col] = new RowExtractor[A, Col, Fixed[Nat._1]] {
    def extract[Row](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[A] = for {
      idx <- cols.headOption
      value <- frame.column[A](idx).apply(row).value
    } yield value
  }
}

trait RowExtractorLow3 extends RowExtractorLow2 {
  implicit def hnilRowExtractor[Col] = new RowExtractor[HNil, Col, Fixed[_0]] {
    def extract[Row](frame: Frame[Row,Col], row: Row, cols: List[Col]): Option[HNil] = cols match {
      case Nil => Some(HNil)
      case _ => None
    }
  }
}

object RowExtractor extends RowExtractorLow3 {
  implicit def hlistRowExtractor[H: Typeable: TypeTag, T <: HList, Col, N <: Nat](implicit
      tailExtractor: RowExtractor[T, Col, Fixed[N]]) = new RowExtractor[H :: T, Col, Fixed[Succ[N]]] {
    def extract[Row](frame: Frame[Row,Col], row: Row, colIndices: List[Col]): Option[H :: T] = for {
      idx <- colIndices.headOption
      tail <- tailExtractor.extract(frame, row, colIndices.tail)
      value <- frame.column[H](idx).apply(row).value
    } yield {
      value :: tail
    }
  }

  def extract[Row, Col, A, S <: Size](frame: Frame[Row,Col], row: Row)(implicit extractor: RowExtractor[A, Col, S]): Option[A] =
    extractor.extract(frame, row, frame.colIndex.keys.toList)

  def extract[Row, Col, A, S <: Size](frame: Frame[Row,Col], row: Row, cols: List[Col])(implicit extractor: RowExtractor[A, Col, S]): Option[A] =
    extractor.extract(frame, row, cols)
}
