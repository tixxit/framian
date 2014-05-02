package pellucid
package pframe

import scala.reflect.ClassTag

import spire.algebra._
import spire.syntax.additiveMonoid._

import shapeless._
import shapeless.ops.function._

trait RowExtractor[A, Col, RowSize <: Size] {
  type P
  def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[P]
  def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[A]
}

trait RowExtractorLow0 {
  implicit def variableExtractorIsFixed[A, Col, N <: Nat](implicit e: RowExtractor[A, Col, Variable]) =
    new RowExtractor[A, Col, Fixed[N]] {
      type P = e.P
      def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[P] =
        e.prepare(frame, cols)
      def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[A] =
        e.extract(frame, key, row, p)
    }
}

trait RowExtractorLow1 {
  implicit def generic[A, B, Col, S <: Size](implicit generic: Generic.Aux[A, B],
      extractor: RowExtractor[B, Col, S]) =
    new RowExtractor[A,Col,S] {
      type P = extractor.P
      def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[P] =
        extractor.prepare(frame, cols)
      def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[A] =
        extractor.extract(frame, key, row, p) map (generic.from(_))
    }
}

trait RowExtractorLow2 extends RowExtractorLow1 {
  implicit def simpleRowExtractor[A: ColumnTyper, Col] =
    new RowExtractor[A, Col, Fixed[Nat._1]] {
      type P = Column[A]
      def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[Column[A]] =
        cols.headOption flatMap { idx =>
          frame.columnsAsSeries(idx).map(_.cast[A]).value
        }
      def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, col: Column[A]): Cell[A] =
        col(row)
    }
}

trait RowExtractorLow3 extends RowExtractorLow2 {
  implicit def hnilRowExtractor[Col] = new RowExtractor[HNil, Col, Fixed[_0]] {
    type P = Unit
    def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[Unit] =
      if (cols.isEmpty) Some(()) else None
    def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[HNil] =
      Value(HNil)
  }
}

object RowExtractor extends RowExtractorLow3 {
  implicit def hlistRowExtractor[H: ColumnTyper, T <: HList, Col, N <: Nat](implicit
      te: RowExtractor[T, Col, Fixed[N]]) =
    new RowExtractor[H :: T, Col, Fixed[Succ[N]]] {
      type P = (Column[H], te.P)

      def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[P] = for {
        idx <- cols.headOption
        tail <- te.prepare(frame, cols.tail)
        col <- frame.columnsAsSeries(idx).value
      } yield (col.cast[H] -> tail)

      def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[H :: T] = {
        val (col, tp) = p
        for {
          tail <- te.extract(frame, key, row, tp)
          value <- col(row)
        } yield {
          value :: tail
        }
      }
    }

  final def apply[A, C, S <: Size](implicit e: RowExtractor[A, C, S]) = e

  def listOf[A, Col](implicit e: RowExtractor[A, Col, Fixed[Nat._1]]) = new RowExtractor[List[Cell[A]], Col, Variable] {
    type P = List[e.P]

    def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[List[e.P]] =
      cols.foldRight(Some(Nil): Option[List[e.P]]) { (col, acc) =>
        acc flatMap { ps => e.prepare(frame, col :: Nil).map(_ :: ps) }
      }

    def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, ps: List[e.P]): Cell[List[Cell[A]]] =
      Value(ps.map { p => e.extract(frame, key, row, p) })
  }
}
