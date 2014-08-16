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
import scala.reflect.runtime.universe.{ TypeTag, typeTag }
import scala.annotation.tailrec
import scala.collection.{ IterableLike, Iterable }
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ ArrayBuilder, Builder }

import spire.algebra.{ Order, Eq }
import spire.math.Searching

import shapeless._

import spire.syntax.order._
import spire.syntax.cfor._

sealed abstract class Index[K](implicit val order: Order[K], val classTag: ClassTag[K])
    extends Iterable[(K, Int)] with IterableLike[(K, Int), Index[K]] {

  def empty: Index[K] = Index.empty[K]

  def size: Int

  def iterator: Iterator[(K, Int)]

  def search(k: K): Int = {
    val i = Searching.search(keys, k)
    if (i < 0) {
      val j = -i - 1
      if (j < indices.length) {
        -indices(-i - 1) - 1
      } else {
        -j - 1
      }
    } else {
      indices(i)
    }
  }

  def apply(i: Int): (K, Int) = (keyAt(i), indexAt(i))
  def keyAt(i: Int): K
  def indexAt(i: Int): Int

  def foreach[U](f: (K, Int) => U): Unit

  override def foreach[U](f: ((K, Int)) => U): Unit = foreach(Function.untupled(f))

  override def seq: Index[K] = this

  override protected def newBuilder: Builder[(K, Int), Index[K]] =
    new Index.IndexBuilder

  def get(k: K): Option[Int] = {
    val i = search(k)
    if (i >= 0) Some(i) else None
  }

  def getAll(k: K): Index[K] = {
    @tailrec def findLower(j: Int): Int =
      if (j > 0 && keys(j - 1) === k) findLower(j - 1)
      else j

    @tailrec def findUpper(j: Int): Int =
      if (j < keys.length && keys(j) === k) findUpper(j + 1)
      else j

    val i = search(k)
    if (i >= 0) {
      val lb = findLower(i)
      val ub = findUpper(i + 1)
      Index.ordered(keys.slice(lb, ub), indices.slice(lb, ub))
    } else {
      Index.empty[K]
    }
  }

  def reverse: Index[K] = {
    val keys0 = new Array[K](keys.length)
    val indices0 = new Array[Int](indices.length)
    cfor(0)(_ < keys.length, _ + 1) { i =>
      val j = keys.length - i - 1
      keys0(i) = keyAt(j)
      indices0(i) = indexAt(j)
    }
    Index(keys0, indices0)
  }

  def sorted: OrderedIndex[K] = Index.ordered(keys, indices)
  /*def sortWith(f: ((K, Int), (K, Int)) => Boolean) = {
    val ind = keys.zip(indices)
    val sorted = ind.sortWith(f)
    val (ks, ids) = sorted.unzip

    Index.unordered(keys, indices)
  }*/

  def resetIndices: Index[K]

  protected[framian] def isOrdered: Boolean
  // These must contain both the keys and the indices, in sorted order.
  private[framian] def keys: Array[K]
  private[framian] def indices: Array[Int]
  private[framian] def withIndices(is: Array[Int]): Index[K]
}

object Index {
  implicit def cbf[K: Order: ClassTag]: CanBuildFrom[Index[_], (K, Int), Index[K]] =
    new CanBuildFrom[Index[_], (K, Int), Index[K]] {
      def apply(): Builder[(K, Int), Index[K]] = new IndexBuilder[K]
      def apply(from: Index[_]): Builder[(K, Int), Index[K]] = apply()
    }

  def empty[K: Order: ClassTag]: Index[K] = new OrderedIndex[K](new Array[K](0), new Array[Int](0))

  def fromKeys[K: Order: ClassTag](keys: K*): Index[K] =
    Index(keys.zipWithIndex: _*)

  def apply[K: Order: ClassTag](keys: Array[K]): Index[K] =
    Index(keys, Array.range(0, keys.length))

  def apply[K: Order: ClassTag](pairs: (K, Int)*): Index[K] = {
    val (keys, indices) = pairs.unzip
    apply(keys.toArray, indices.toArray)
  }

  def apply[K: Order: ClassTag](keys: Array[K], indices: Array[Int]): Index[K] = {
    import spire.syntax.order._

    @tailrec
    def isOrdered(i: Int): Boolean = if (i >= keys.length) {
      true
    } else if (keys(i - 1) > keys(i)) {
      false
    } else {
      isOrdered(i + 1)
    }

    if (isOrdered(1)) {
      ordered(keys, indices)
    } else {
      unordered(keys, indices)
    }
  }

  def ordered[K: Order: ClassTag](keys: Array[K]): OrderedIndex[K] =
    ordered(keys, Array.range(0, keys.length))

  def ordered[K: Order: ClassTag](keys: Array[K], indices: Array[Int]): OrderedIndex[K] =
    new OrderedIndex(keys, indices)

  def unordered[K: Order: ClassTag](keys: Array[K]): Index[K] =
    unordered(keys, Array.range(0, keys.length))

  def unordered[K: Order: ClassTag](keys: Array[K], indices: Array[Int]): Index[K] = {
    import spire.syntax.std.array._

    require(keys.length == indices.length)

    def flip(xs: Array[Int]): Array[Int] = {
      val ys = new Array[Int](xs.size)
      cfor(0)(_ < xs.size, _ + 1) { i =>
        ys(xs(i)) = i
      }
      ys
    }

    def shuffle[A: ClassTag](xs: Array[A], order: Array[Int]): Array[A] = {
      val ys = new Array[A](xs.length)
      cfor(0)(_ < order.length, _ + 1) { i =>
        ys(i) = xs(order(i))
      }
      ys
    }

    val order0 = Array.range(0, keys.length).qsortedBy(keys(_))
    val indices0 = shuffle(indices, order0)
    val keys0 = shuffle(keys, order0)
    new UnorderedIndex(keys0, indices0, flip(order0))
  }

  private final class IndexBuilder[K: Order: ClassTag] extends Builder[(K, Int), Index[K]] {
    val keys = ArrayBuilder.make[K]()
    val indices = ArrayBuilder.make[Int]()

    def +=(elem: (K, Int)) = {
      keys += elem._1
      indices += elem._2
      this
    }

    def clear(): Unit = {
      keys.clear()
      indices.clear()
    }

    def result(): Index[K] = Index.unordered(keys.result(), indices.result())
  }

  @tailrec
  private final def spanEnd[K: Eq](keys: Array[K], key: K, i: Int): Int =
    if (i < keys.length && keys(i) === key) spanEnd(keys, key, i + 1)
    else i

  trait Grouper[K] {
    type State

    def init: State

    def group(state: State)(keys: Array[K], indices: Array[Int], start: Int, end: Int): State
  }

  def group[K: Order](index: Index[K])(grouper: Grouper[K]): grouper.State = {
    val keys = index.keys
    val indices = index.indices

    @tailrec def loop(s0: grouper.State, start: Int): grouper.State =
      if (start < keys.length) {
        val end = spanEnd(keys, keys(start), start)
        val s1 = grouper.group(s0)(keys, indices, start, end)
        loop(s1, end)
      } else {
        s0
      }

    loop(grouper.init, 0)
  }

  /**
   * `Cogrouper` provides the abstraction used by `Index.cogroup` to work with
   * the result of a cogroup. Essentially, the cogroup will initiate some state,
   * then perform a cogroup on the 2 [[Index]]es, with each unique key resulting
   * in one call to `cogroup`. The reason the signature is a bit weird, with
   * the start/end offsets being passed in is to avoid copying and allocation
   * where possible. All implementations of [[Index]] can perform this operation
   * efficiently.
   */
  trait Cogrouper[K] {
    type State

    def init: State

    def cogroup(state: State)(
        lKeys: Array[K], lIdx: Array[Int], lStart: Int, lEnd: Int,
        rKeys: Array[K], rIdx: Array[Int], rStart: Int, rEnd: Int): State
  }

  abstract class GenericJoin[K: ClassTag] extends Cogrouper[K] {
    // We cheat here and use a mutable state because an immutable one would just
    // be too slow.
    final class State {
      val keys: ArrayBuilder[K] = ArrayBuilder.make[K]
      val lIndices: ArrayBuilder[Int] = ArrayBuilder.make[Int]
      val rIndices: ArrayBuilder[Int] = ArrayBuilder.make[Int]

      def add(k: K, l: Int, r: Int) { keys += k; lIndices += l; rIndices += r }

      def result(): (Array[K], Array[Int], Array[Int]) =
        (keys.result(), lIndices.result(), rIndices.result())
    }

    def init = new State
  }

  object GenericJoin {
    final def Skip = Int.MinValue
  }

  def cogroup[K: Order](lhs: Index[K], rhs: Index[K])
      (cogrouper: Cogrouper[K]): cogrouper.State = {
    val lKeys = lhs.keys
    val lIndices = lhs.indices
    val rKeys = rhs.keys
    val rIndices = rhs.indices

    @tailrec def loop(s0: cogrouper.State, lStart: Int, rStart: Int): cogrouper.State =
      if (lStart < lKeys.length && rStart < rKeys.length) {
        val lKey = lKeys(lStart)
        val rKey = rKeys(rStart)
        val ord = lKey compare rKey
        val lEnd = if (ord <= 0) spanEnd(lKeys, lKey, lStart + 1) else lStart
        val rEnd = if (ord >= 0) spanEnd(rKeys, rKey, rStart + 1) else rStart
        val s1 = cogrouper.cogroup(s0)(lKeys, lIndices, lStart, lEnd, rKeys, rIndices, rStart, rEnd)
        loop(s1, lEnd, rEnd)
      } else if (lStart < lKeys.length) {
        val lEnd = spanEnd(lKeys, lKeys(lStart), lStart + 1)
        val s1 = cogrouper.cogroup(s0)(lKeys, lIndices, lStart, lEnd, rKeys, rIndices, rStart, rStart)
        loop(s1, lEnd, rStart)
      } else if (rStart < rKeys.length) {
        val rEnd = spanEnd(rKeys, rKeys(rStart), rStart + 1)
        val s1 = cogrouper.cogroup(s0)(lKeys, lIndices, lStart, lStart, rKeys, rIndices, rStart, rEnd)
        loop(s1, lStart, rEnd)
      } else {
        s0
      }

    loop(cogrouper.init, 0, 0)
  }
}

final case class UnorderedIndex[K: Order: ClassTag](
      keys: Array[K], indices: Array[Int], ord: Array[Int])
    extends Index[K] {

  override def size: Int = keys.size

  def keyAt(i: Int): K = keys(ord(i))
  def indexAt(i: Int): Int = indices(ord(i))

  def iterator: Iterator[(K, Int)] = ord.iterator map { i =>
    (keys(i), indices(i))
  }

  def foreach[U](f: (K, Int) => U): Unit = {
    cfor(0)(_ < ord.length, _ + 1) { i =>
      val j = ord(i)
      f(keys(j), indices(j))
    }
  }

  def resetIndices: Index[K] = UnorderedIndex(keys, Array.range(0, keys.size), ord)

  protected[framian] def isOrdered = false

  private[framian] def withIndices(is: Array[Int]): Index[K] = UnorderedIndex(keys, is, ord)
}

final class OrderedIndex[K: Order: ClassTag](
      private[framian] val keys: Array[K],
      private[framian] val indices: Array[Int])
    extends Index[K] {

  override def size: Int = keys.size
  def keyAt(i: Int): K = keys(i)
  def indexAt(i: Int): Int = indices(i)
  def iterator: Iterator[(K, Int)] = Iterator.tabulate(keys.length) { i =>
    (keys(i), indices(i))
  }
  def foreach[U](f: (K, Int) => U): Unit = {
    cfor(0)(_ < keys.length, _ + 1) { i =>
      f(keys(i), indices(i))
    }
  }
  def resetIndices: Index[K] = OrderedIndex(keys, Array.range(0, keys.size))

  protected[framian] def isOrdered = true

  private[framian] def withIndices(is: Array[Int]): Index[K] = OrderedIndex(keys, is)
  lazy val isSequential: Boolean = {
    var isSeq = true
    var i = 0
    while (i < indices.length && isSeq) {
      isSeq = isSeq && indices(i) == i
      i += 1
    }
    isSeq
  }
}
