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

  def map[B](f: A => B): AxisSelection[K, B]
  def filter(f: A => Boolean): AxisSelection[K, A]
  def recoverWith(pf: PartialFunction[NonValue, Cell[A]]): AxisSelection[K, A]
  def recover(pf: PartialFunction[NonValue, A]): AxisSelection[K, A]
}

trait AxisSelectionLike[K, A, This[K, A] <: AxisSelectionLike[K, A, This]] extends AxisSelection[K, A] {
  def mapCell[B](f: Cell[A] => Cell[B]): This[K, B]
  def orElse(that: This[K, A]): This[K, A]

  def map[B](f: A => B): This[K, B] = mapCell(_ map f)
  def filter(p: A => Boolean): This[K, A] = mapCell(_ filter p)
  def recoverWith(pf: PartialFunction[NonValue, Cell[A]]): This[K, A] = mapCell(_ recoverWith pf)
  def recover(pf: PartialFunction[NonValue, A]): This[K, A] = mapCell(_ recover pf)
}

trait AxisSelectionCompanion[AxisSelection[K, A] <: AxisSelectionLike[K, A, AxisSelection]] {
  sealed trait SizedAxisSelection[K, Sz <: Size, A] extends AxisSelectionLike[K, A, AxisSelection] { self: AxisSelection[K, A] =>
    val extractor: RowExtractor[A, K, Sz]

    def getOrElse(all: => List[K]): List[K] = fold(all)(keys => keys)

    def orElse(that: AxisSelection[K, A]): AxisSelection[K, A] = OrElse[K, A, A](this, that, identity)

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
    def as[B](implicit extractor0: RowExtractor[B, K, Sz]): AxisSelection[K, B]
  }

  type All[K, A] <: AllAxisSelection[K, A] with AxisSelection[K, A]
  def All: AllCompanion
  trait AllCompanion {
    def apply[K, A](extractor: RowExtractor[A, K, Variable]): All[K, A]
  }

  type Pick[K, S <: Size, A] <: PickAxisSelection[K, S, A] with AxisSelection[K, A]
  def Pick: PickCompanion
  trait PickCompanion {
    def apply[K, S <: Size, A](keys: List[K], extractor: RowExtractor[A, K, S]): Pick[K, S, A]
  }

  type OrElse[K, A, B] <: OrElseAxisSelection[K, A, B] with AxisSelection[K, B]
  def OrElse: OrElseCompanion
  trait OrElseCompanion {
    def apply[K, A, B](fst: AxisSelection[K, A], snd: AxisSelection[K, A], f: Cell[A] => Cell[B]): OrElse[K, A, B]
  }

  trait AllAxisSelection[K, A] extends SizedAxisSelection[K, Variable, A] { self: AxisSelection[K, A] =>
    val extractor: RowExtractor[A, K, Variable]
    def fold[B](all: => B)(f: List[K] => B): B = all

    def mapCell[B](f: Cell[A] => Cell[B]): All[K, B] =
      All(extractor mapCell f)

    def as[B](implicit extractor0: RowExtractor[B, K, Variable]): AxisSelection[K, B] =
      All(extractor0)
    def asListOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): AxisSelection[K, List[Cell[B]]] =
      as(RowExtractor.collectionOf)
    def asVectorOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): AxisSelection[K, Vector[Cell[B]]] =
      as(RowExtractor.collectionOf)
    def asArrayOf[B: ClassTag](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): AxisSelection[K, Array[B]] =
      as(RowExtractor.denseCollectionOf)
  }

  trait PickAxisSelection[K, S <: Size, A] extends SizedAxisSelection[K, S, A] { self: AxisSelection[K, A] =>
    val keys: List[K]
    val extractor: RowExtractor[A, K, S]

    def fold[B](all: => B)(f: List[K] => B): B = f(keys)

    def mapCell[B](f: Cell[A] => Cell[B]): Pick[K, S, B] =
      Pick(keys, extractor mapCell f)

    def variable: Pick[K, Variable, A] = this.asInstanceOf[Pick[K, Variable, A]]

    def as[B](implicit extractor0: RowExtractor[B, K, S]): AxisSelection[K, B] =
      Pick(keys, extractor0)
    def asListOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): AxisSelection[K, List[Cell[B]]] =
      variable.as(RowExtractor.collectionOf)
    def asVectorOf[B](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): AxisSelection[K, Vector[Cell[B]]] =
      variable.as(RowExtractor.collectionOf)
    def asArrayOf[B: ClassTag](implicit extractor0: RowExtractor[B, K, Fixed[Nat._1]]): AxisSelection[K, Array[B]] =
      variable.as(RowExtractor.denseCollectionOf)
  }

  trait OrElseAxisSelection[K, I, A] extends AxisSelectionLike[K, A, AxisSelection] { self: AxisSelection[K, A] =>
    def fst: AxisSelection[K, I]
    def snd: AxisSelection[K, I]
    def k: Cell[I] => Cell[A]

    def orElse(that: AxisSelection[K, A]): AxisSelection[K, A] = OrElse[K, A, A](this, that, identity)

    def extractorFor(cols: Series[K, UntypedColumn]): (Int => Cell[A]) = {
      val get1 = fst.extractorFor(cols)
      val get2 = snd.extractorFor(cols)

      { row => k(get1(row) orElse get2(row)) }
    }

    def mapCell[B](f: Cell[A] => Cell[B]): OrElse[K, I, B] =
      OrElse(fst, snd, k andThen f)
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
