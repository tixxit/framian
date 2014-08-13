/*  _____                    _
 * |  ___| __ __ _ _ __ ___ (_) __ _ _ __
 * | |_ | '__/ _` | '_ ` _ \| |/ _` | '_ \
 * |  _|| | | (_| | | | | | | | (_| | | | |
 * |_|  |_|  \__,_|_| |_| |_|_|\__,_|_| |_|
 *
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package framian

import scala.reflect.ClassTag
import scala.collection.generic.CanBuildFrom

import spire.algebra._
import spire.syntax.additiveMonoid._

import shapeless._
import shapeless.ops.function._

trait RowExtractor[A, Col, RowSize <: Size] {
  type P
  def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[P]
  def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[A]
  def map[B](f: A => B): RowExtractor[B, Col, RowSize] = new MappedRowExtractor(this, f)
}

private final class MappedRowExtractor[A, B, Col, RowSize <: Size](val e: RowExtractor[A, Col, RowSize], f: A => B)
    extends RowExtractor[B, Col, RowSize] {
  type P = e.P
  def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[P] =
    e.prepare(frame, cols)
  def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, p: P): Cell[B] =
    e.extract(frame, key, row, p) map f
}


object RowExtractorLowPriorityImplicits {
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

  trait RowExtractorLow1 extends RowExtractorLow0 {
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
}

trait RowExtractorLowPriorityImplicits extends RowExtractorLowPriorityImplicits.RowExtractorLow3


object RowExtractor extends RowExtractorLowPriorityImplicits {
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

  def collectionOf[CC[_], A, Col](implicit e: RowExtractor[A, Col, Fixed[Nat._1]], cbf: CanBuildFrom[Nothing, Cell[A], CC[Cell[A]]]) = new RowExtractor[CC[Cell[A]], Col, Variable] {
    type P = List[e.P]

    def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[List[e.P]] = {
      cols.foldRight(Some(Nil): Option[List[e.P]]) { (col, acc) =>
        acc flatMap { ps => e.prepare(frame, col :: Nil).map(_ :: ps) }
      }
    }

    def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, ps: List[e.P]): Cell[CC[Cell[A]]] = {
      val bldr = cbf()
      ps foreach { p =>
        bldr += e.extract(frame, key, row, p)
      }
      Value(bldr.result())
    }
  }

  def denseCollectionOf[CC[_], A, Col](implicit e: RowExtractor[A, Col, Fixed[Nat._1]], cbf: CanBuildFrom[Nothing, A, CC[A]]) = new RowExtractor[CC[A], Col, Variable] {
    type P = List[e.P]

    def prepare[Row](frame: Frame[Row, Col], cols: List[Col]): Option[List[e.P]] = {
      cols.foldRight(Some(Nil): Option[List[e.P]]) { (col, acc) =>
        acc flatMap { ps => e.prepare(frame, col :: Nil).map(_ :: ps) }
      }
    }

    def extract[Row](frame: Frame[Row, Col], key: Row, row: Int, ps: List[e.P]): Cell[CC[A]] = {
      val bldr = cbf()
      ps foreach { p =>
        e.extract(frame, key, row, p) match {
          case Value(a) => bldr += a
          case (missing: NonValue) => return missing
        }
      }
      Value(bldr.result())
    }
  }
}
