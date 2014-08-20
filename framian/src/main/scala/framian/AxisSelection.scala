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

import scala.language.higherKinds

import scala.collection.SortedMap
import scala.reflect.ClassTag

import spire.algebra._
import spire.implicits._

import shapeless._
import shapeless.ops.function._
import shapeless.ops.nat._

trait AxisSelection[K, A] {
  def extractorFor(cols: Series[K, UntypedColumn]): (Int => Cell[A])

  def foreach[I, U](index: Index[I], cols: Series[K, UntypedColumn])(f: (I, Int, Cell[A]) => U): Unit = {
    val get = extractorFor(cols)
    index.foreach { (key, row) =>
      f(key, row, get(row))
    }
  }

  def mapCell[B](f: Cell[A] => Cell[B]): AxisSelection[K, B]

  def map[B](f: A => B): AxisSelection[K, B]
  def filter(f: A => Boolean): AxisSelection[K, A]
  def recoverWith(pf: PartialFunction[NonValue, Cell[A]]): AxisSelection[K, A]
  def recover(pf: PartialFunction[NonValue, A]): AxisSelection[K, A]
}

trait AxisSelectionLike[K, A, This[K, A] <: AxisSelectionLike[K, A, This]] extends AxisSelection[K, A] {
  def mapCell[B](f: Cell[A] => Cell[B]): This[K, B]
  def orElse(that: This[K, A]): This[K, A]
  def flatMap[B](f: A => This[K, B]): This[K, B]
  def zipWith[B, C](that: This[K, B])(f: (A, B) => C): This[K, C]

  def map[B](f: A => B): This[K, B] = mapCell(_ map f)
  def filter(p: A => Boolean): This[K, A] = mapCell(_ filter p)
  def recoverWith(pf: PartialFunction[NonValue, Cell[A]]): This[K, A] = mapCell(_ recoverWith pf)
  def recover(pf: PartialFunction[NonValue, A]): This[K, A] = mapCell(_ recover pf)
  def zip[B](that: This[K, B]): This[K, (A, B)] = zipWith(that)(_ -> _)
}

trait AxisSelectionCompanion[Sel[K, A] <: AxisSelectionLike[K, A, Sel]] {

  type All[K, A] <: AllAxisSelection[K, A] with Sel[K, A]
  def All: AllCompanion
  trait AllCompanion {
    def apply[K, A](extractor: RowExtractor[A, K, Variable]): All[K, A]
  }

  type Pick[K, S <: Size, A] <: PickAxisSelection[K, S, A] with Sel[K, A]
  def Pick: PickCompanion
  trait PickCompanion {
    def apply[K, S <: Size, A](keys: List[K], extractor: RowExtractor[A, K, S]): Pick[K, S, A]
  }

  type Wrapped[K, A] <: WrappedAxisSelection[K, A] with Sel[K, A]
  def Wrapped: WrappedCompanion
  trait WrappedCompanion {
    def apply[K, A](sel: AxisSelection[K, A]): Wrapped[K, A]
  }

  sealed trait Bridge[K, A] extends AxisSelectionLike[K, A, Sel] { self: Sel[K, A] =>
    def orElse(that: Sel[K, A]): Sel[K, A] =
      Wrapped(ops.OrElse[K, A](this, that))
    def flatMap[B](f: A => Sel[K, B]): Sel[K, B] =
      Wrapped(ops.Bind[K, A, B](this, f))
    def zipWith[B, C](that: Sel[K, B])(f: (A, B) => C): Sel[K, C] =
      Wrapped(ops.Zipped[K, A, B, C](this, that, (a, b) => a.zipMap(b)(f)))
  }

  sealed trait SizedAxisSelection[K, Sz <: Size, A] extends Bridge[K, A] { self: Sel[K, A] =>
    val extractor: RowExtractor[A, K, Sz]

    def getOrElse(all: => List[K]): List[K] = fold(all)(keys => keys)

    def extractorFor(cols: Series[K, UntypedColumn]): (Int => Cell[A]) = {
      val colKeys = getOrElse(cols.index.keys.toList)
      extractor.prepare(cols, colKeys) match {
        case Some(p) => { row => extractor.extract(row, p) }
        case None => { row => NA }
      }
    }

    override def foreach[I, U](index: Index[I], cols: Series[K, UntypedColumn])(f: (I, Int, Cell[A]) => U): Unit = {
      val colKeys = getOrElse(cols.index.keys.toList)
      for (p <- extractor.prepare(cols, colKeys)) {
        index foreach { (key, row) =>
          f(key, row, extractor.extract(row, p))
        }
      }
    }

    def fold[B](all: => B)(f: List[K] => B): B
    def as[B](implicit extractor0: RowExtractor[B, K, Sz]): Sel[K, B]
  }

  trait AllAxisSelection[K, A] extends SizedAxisSelection[K, Variable, A] { self: Sel[K, A] =>
    val extractor: RowExtractor[A, K, Variable]
    def fold[B](all: => B)(f: List[K] => B): B = all

    def mapCell[B](f: Cell[A] => Cell[B]): All[K, B] =
      All(extractor mapCell f)

    def as[B](implicit extractor0: RowExtractor[B, K, Variable]): Sel[K, B] =
      All(extractor0)
    def asListOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): Sel[K, List[Cell[B]]] =
      as(RowExtractor.collectionOf)
    def asVectorOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): Sel[K, Vector[Cell[B]]] =
      as(RowExtractor.collectionOf)
    def asArrayOf[B: ClassTag](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): Sel[K, Array[B]] =
      as(RowExtractor.denseCollectionOf)
  }

  trait PickAxisSelection[K, S <: Size, A] extends SizedAxisSelection[K, S, A] { self: Sel[K, A] =>
    val keys: List[K]
    val extractor: RowExtractor[A, K, S]

    def fold[B](all: => B)(f: List[K] => B): B = f(keys)

    def mapCell[B](f: Cell[A] => Cell[B]): Pick[K, S, B] =
      Pick(keys, extractor mapCell f)

    def variable: Pick[K, Variable, A] = this.asInstanceOf[Pick[K, Variable, A]]

    def as[B](implicit extractor0: RowExtractor[B, K, S]): Sel[K, B] =
      Pick(keys, extractor0)
    def asListOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): Sel[K, List[Cell[B]]] =
      variable.as(RowExtractor.collectionOf)
    def asVectorOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): Sel[K, Vector[Cell[B]]] =
      variable.as(RowExtractor.collectionOf)
    def asArrayOf[B: ClassTag](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): Sel[K, Array[B]] =
      variable.as(RowExtractor.denseCollectionOf)
  }

  trait WrappedAxisSelection[K, A] extends Bridge[K, A] { self: Sel[K, A] =>
    def sel: AxisSelection[K, A]

    def mapCell[B](f: Cell[A] => Cell[B]): Sel[K, B] =
      Wrapped(sel.mapCell(f))

    def extractorFor(cols: Series[K, UntypedColumn]): (Int => Cell[A]) =
      sel.extractorFor(cols)

    override def foreach[I, U](index: Index[I], cols: Series[K, UntypedColumn])(f: (I, Int, Cell[A]) => U): Unit =
      sel.foreach(index, cols)(f)
  }

  object ops {
    trait Op[K, A] extends AxisSelection[K, A] {
      def mapCell[B](f: Cell[A] => Cell[B]): AxisSelection[K, B]

      def map[B](f: A => B): AxisSelection[K, B] = mapCell(_ map f)
      def filter(p: A => Boolean): AxisSelection[K, A] = mapCell(_ filter p)
      def recoverWith(pf: PartialFunction[NonValue, Cell[A]]): AxisSelection[K, A] = mapCell(_ recoverWith pf)
      def recover(pf: PartialFunction[NonValue, A]): AxisSelection[K, A] = mapCell(_ recover pf)
    }

    case class Zipped[K, A, B, C](
      fst: AxisSelection[K, A],
      snd: AxisSelection[K, B],
      combine: (Cell[A], => Cell[B]) => Cell[C]
    ) extends Op[K, C] {

      def extractorFor(cols: Series[K, UntypedColumn]): (Int => Cell[C]) = {
        val get1 = fst.extractorFor(cols)
        val get2 = snd.extractorFor(cols)

        { row => combine(get1(row), get2(row)) }
      }

      def mapCell[D](f: Cell[C] => Cell[D]): AxisSelection[K, D] =
        Zipped[K, A, B, D](fst, snd, { (a, b) => f(combine(a, b)) })
    }

    def OrElse[K, A](fst: AxisSelection[K, A], snd: AxisSelection[K, A]): AxisSelection[K, A] =
      Zipped[K, A, A, A](fst, snd, _ orElse _)

    case class Bind[K, A, B](
      sel: AxisSelection[K, A],
      k: A => AxisSelection[K, B]
    ) extends Op[K, B] {
      def extractorFor(cols: Series[K, UntypedColumn]): (Int => Cell[B]) = {
        val get = sel.extractorFor(cols)

        { row => get(row).map(k).flatMap(_.extractorFor(cols)(row)) }
      }

      def mapCell[C](f: Cell[B] => Cell[C]): AxisSelection[K, C] =
        Bind[K, A, C](sel, { a => k(a).mapCell(f) })
    }
  }

  import Nat._

  def all[K] = All[K, Rec[K]](RowExtractor[Rec[K], K, Variable])

  def unsized[K](cols: Seq[K]) =
    Pick(cols.toList, RowExtractor[Rec[K], K, Variable])

  def sized[K, N <: Nat](s: Sized[List[K], N]): Pick[K, Fixed[N], Rec[K]] =
    Pick(s.unsized, RowExtractor[Rec[K], K, Fixed[N]])

  def apply[K](c0: K): Pick[K, Fixed[_1], Rec[K]] =
    sized(Sized[List](c0))

  def apply[K](c0: K, c1: K): Pick[K, Fixed[_2], Rec[K]] =
    sized(Sized[List](c0, c1))

  def apply[K](c0: K, c1: K, c2: K, c3: K): Pick[K, Fixed[_4], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K): Pick[K, Fixed[_5], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K): Pick[K, Fixed[_6], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K): Pick[K, Fixed[_7], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K): Pick[K, Fixed[_8], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K, c8: K): Pick[K, Fixed[_9], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7, c8))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K, c8: K, c9: K): Pick[K, Fixed[_10], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7, c8, c9))
}
