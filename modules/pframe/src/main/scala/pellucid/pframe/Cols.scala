package pellucid.pframe

import scala.language.higherKinds

import scala.annotation.implicitNotFound
import scala.collection.SortedMap
import scala.reflect.ClassTag

import spire.algebra._
import spire.implicits._

import shapeless._
import shapeless.ops.function._
import shapeless.ops.nat._

sealed trait Cols[K, A] {
  val extractor: RowExtractor[A, K, _]
  def getOrElse(all: => List[K]) = fold(all)(keys => keys)
  def fold[B](all: => B)(f: List[K] => B): B
  def map[B](f: A => B): Cols[K, B]
}

sealed trait SizedCols[K, Sz <: Size, A] extends Cols[K, A] {
  val extractor: RowExtractor[A, K, Sz]

  def fold[B](all: => B)(f: List[K] => B): B
  def as[B](implicit extractor0: RowExtractor[B, K, Sz]): SizedCols[K, Sz, B]
}

final class AllCols[K, A](val extractor: RowExtractor[A, K, Variable])
    extends SizedCols[K, Variable, A] {
  def fold[B](all: => B)(f: List[K] => B): B = all
  def as[B](implicit extractor0: RowExtractor[B, K, Variable]): AllCols[K, B] =
    new AllCols[K, B](extractor0)
  def map[B](f: A => B): AllCols[K, B] =
    new AllCols[K, B](extractor map f)
}

final class SomeCols[K, S <: Size, A](val keys: List[K], val extractor: RowExtractor[A, K, S])
    extends SizedCols[K, S, A] {
  def fold[B](all: => B)(f: List[K] => B): B = f(keys)
  def as[B](implicit extractor0: RowExtractor[B, K, S]): SomeCols[K, S, B] =
    new SomeCols[K, S, B](keys, extractor0)
  def map[B](f: A => B): SomeCols[K, S, B] =
    new SomeCols[K, S, B](keys, extractor map f)
}

object Cols extends ColsFunctions

trait ColsFunctions extends SizedConstructor[SomeCols] {
  def all[K] = new AllCols[K, Rec[K]](RowExtractor[Rec[K], K, Variable])

  def sized[K, N <: Nat](s: Sized[List[K], N]): SomeCols[K, Fixed[N], Rec[K]] =
    new SomeCols[K, Fixed[N], Rec[K]](s.unsized, RowExtractor[Rec[K], K, Fixed[N]])
}
