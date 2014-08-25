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

trait RowExtractor[A, K, Sz <: Size] {
  type P
  def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P]
  def extract(row: Int, p: P): Cell[A]

  def cellMap[B](f: Cell[A] => Cell[B]): RowExtractor[B, K, Sz] =
    new MappedRowExtractor[A, B, K, Sz](this, f)

  def map[B](f: A => B): RowExtractor[B, K, Sz] =
    cellMap(_ map f)

  def filter(p: A => Boolean): RowExtractor[A, K, Sz] =
    cellMap(_ filter p)

  def recover(pf: PartialFunction[NonValue, A]): RowExtractor[A, K, Sz] =
    cellMap(_ recover pf)

  def recoverWith(pf: PartialFunction[NonValue, Cell[A]]): RowExtractor[A, K, Sz] =
    cellMap(_ recoverWith pf)
}

private final class MappedRowExtractor[A, B, K, Sz <: Size](val e: RowExtractor[A, K, Sz], f: Cell[A] => Cell[B])
    extends RowExtractor[B, K, Sz] {
  type P = e.P
  def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P] =
    e.prepare(cols, keys)
  def extract(row: Int, p: P): Cell[B] =
    f(e.extract(row, p))
}

object RowExtractorLowPriorityImplicits {
  trait RowExtractorLow0 {
    implicit def variableExtractorIsFixed[A, K, N <: Nat](implicit e: RowExtractor[A, K, Variable]) =
      new RowExtractor[A, K, Fixed[N]] {
        type P = e.P
        def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P] =
          e.prepare(cols, keys)
        def extract(row: Int, p: P): Cell[A] =
          e.extract(row, p)
      }
  }

  trait RowExtractorLow1 extends RowExtractorLow0 {
    implicit def generic[A, B, K, S <: Size](implicit generic: Generic.Aux[A, B],
        extractor: RowExtractor[B, K, S]) =
      new RowExtractor[A,K,S] {
        type P = extractor.P
        def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P] =
          extractor.prepare(cols, keys)
        def extract(row: Int, p: P): Cell[A] =
          extractor.extract(row, p) map (generic.from(_))
      }
  }

  trait RowExtractorLow2 extends RowExtractorLow1 {
    implicit def simpleRowExtractor[A: ColumnTyper, K] =
      new RowExtractor[A, K, Fixed[Nat._1]] {
        type P = Column[A]
        def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[Column[A]] =
          keys.headOption flatMap { idx => cols(idx).map(_.cast[A]).value }
        def extract(row: Int, col: Column[A]): Cell[A] =
          col(row)
      }
  }

  trait RowExtractorLow3 extends RowExtractorLow2 {
    implicit def hnilRowExtractor[K] = new RowExtractor[HNil, K, Fixed[_0]] {
      type P = Unit
      def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[Unit] =
        if (keys.isEmpty) Some(()) else None
      def extract(row: Int, p: P): Cell[HNil] =
        Value(HNil)
    }
  }
}

trait RowExtractorLowPriorityImplicits extends RowExtractorLowPriorityImplicits.RowExtractorLow3


object RowExtractor extends RowExtractorLowPriorityImplicits {
  implicit def hlistRowExtractor[H: ColumnTyper, T <: HList, K, N <: Nat](implicit
      te: RowExtractor[T, K, Fixed[N]]) =
    new RowExtractor[H :: T, K, Fixed[Succ[N]]] {
      type P = (Column[H], te.P)

      def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P] = for {
        idx <- keys.headOption
        tail <- te.prepare(cols, keys.tail)
        col <- cols(idx).value
      } yield (col.cast[H] -> tail)

      def extract(row: Int, p: P): Cell[H :: T] = {
        val (col, tp) = p
        for {
          tail <- te.extract(row, tp)
          value <- col(row)
        } yield {
          value :: tail
        }
      }
    }

  final def apply[A, C, S <: Size](implicit e: RowExtractor[A, C, S]) = e

  def collectionOf[CC[_], A, K](implicit e: RowExtractor[A, K, Fixed[Nat._1]], cbf: CanBuildFrom[Nothing, Cell[A], CC[Cell[A]]]) = new RowExtractor[CC[Cell[A]], K, Variable] {
    type P = List[e.P]

    def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P] =
      keys.foldRight(Some(Nil): Option[List[e.P]]) { (key, acc) =>
        acc flatMap { ps => e.prepare(cols, key :: Nil).map(_ :: ps) }
      }

    def extract(row: Int, ps: List[e.P]): Cell[CC[Cell[A]]] = {
      val bldr = cbf()
      ps foreach { p =>
        bldr += e.extract(row, p)
      }
      Value(bldr.result())
    }
  }

  def denseCollectionOf[CC[_], A, K](implicit e: RowExtractor[A, K, Fixed[Nat._1]], cbf: CanBuildFrom[Nothing, A, CC[A]]) = new RowExtractor[CC[A], K, Variable] {
    type P = List[e.P]

    def prepare(cols: Series[K, UntypedColumn], keys: List[K]): Option[P] =
      keys.foldRight(Some(Nil): Option[List[e.P]]) { (key, acc) =>
        acc flatMap { ps => e.prepare(cols, key :: Nil).map(_ :: ps) }
      }

    def extract(row: Int, ps: List[e.P]): Cell[CC[A]] = {
      val bldr = cbf()
      ps foreach { p =>
        e.extract(row, p) match {
          case Value(a) => bldr += a
          case (missing: NonValue) => return missing
        }
      }
      Value(bldr.result())
    }
  }
}
