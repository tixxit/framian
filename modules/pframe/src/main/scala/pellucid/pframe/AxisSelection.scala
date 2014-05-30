package pellucid.pframe

import scala.language.higherKinds

import scala.collection.SortedMap
import scala.reflect.ClassTag

import spire.algebra._
import spire.implicits._

import shapeless._
import shapeless.ops.function._
import shapeless.ops.nat._

trait AxisSelectionLike[K, A, +This[K, A] <: AxisSelectionLike[K, A, This]] {
  val extractor: RowExtractor[A, K, _]
  def getOrElse(all: => List[K]) = fold(all)(keys => keys)
  def fold[B](all: => B)(f: List[K] => B): B
  def map[B](f: A => B): This[K, B]
}

trait AxisSelectionCompanion {
  type AxisSelection[K, A] <: AxisSelectionLike[K, A, AxisSelection]

  sealed trait SizedAxisSelection[K, Sz <: Size, A] extends AxisSelectionLike[K, A, AxisSelection] {
    val extractor: RowExtractor[A, K, Sz]

    def fold[B](all: => B)(f: List[K] => B): B
    def as[B](implicit extractor0: RowExtractor[B, K, Sz]): SizedAxisSelection[K, Sz, B]
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

  trait AllAxisSelection[K, A] extends SizedAxisSelection[K, Variable, A] {
    val extractor: RowExtractor[A, K, Variable]
    def fold[B](all: => B)(f: List[K] => B): B = all
    def as[B](implicit extractor0: RowExtractor[B, K, Variable]): All[K, B] =
      All(extractor0)
    def map[B](f: A => B): All[K, B] =
      All(extractor map f)
  }

  trait PickAxisSelection[K, S <: Size, A] extends SizedAxisSelection[K, S, A] {
    val keys: List[K]
    val extractor: RowExtractor[A, K, S]

    def fold[B](all: => B)(f: List[K] => B): B = f(keys)
    def as[B](implicit extractor0: RowExtractor[B, K, S]): Pick[K, S, B] =
      Pick(keys, extractor0)
    def map[B](f: A => B): Pick[K, S, B] =
      Pick(keys, extractor map f)
  }

  import Nat._

  def all[K] = All[K, Rec[K]](RowExtractor[Rec[K], K, Variable])

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

// TODO: Auto-generate.
trait SizedConstructor[SizedCC[_, _ <: Size, _]] {
  import Nat._

  def sized[K, N <: Nat](s: Sized[List[K], N]): SizedCC[K, Fixed[N], Rec[K]]

  def apply[K](c0: K): SizedCC[K, Fixed[_1], Rec[K]] =
    sized(Sized[List](c0))

  def apply[K](c0: K, c1: K): SizedCC[K, Fixed[_2], Rec[K]] =
    sized(Sized[List](c0, c1))

  def apply[K](c0: K, c1: K, c2: K, c3: K): SizedCC[K, Fixed[_4], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K): SizedCC[K, Fixed[_5], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K): SizedCC[K, Fixed[_6], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K): SizedCC[K, Fixed[_7], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K): SizedCC[K, Fixed[_8], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K, c8: K): SizedCC[K, Fixed[_9], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7, c8))

  def apply[K](c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K, c8: K, c9: K): SizedCC[K, Fixed[_10], Rec[K]] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7, c8, c9))
}

