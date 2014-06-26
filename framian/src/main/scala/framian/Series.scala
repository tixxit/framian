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

import language.higherKinds

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ ArrayBuilder, Builder }
import scala.collection.{ IterableLike, Iterable }
import scala.reflect.ClassTag

import spire.algebra._
import spire.math._
import spire.std.int._
import spire.syntax.additiveMonoid._
import spire.syntax.monoid._
import spire.syntax.order._
import spire.syntax.cfor._

import spire.compat._

import framian.reduce.Reducer
import framian.util.TrivialMetricSpace

final class Series[K,V](val index: Index[K], val column: Column[V]) {

  private implicit def classTag = index.classTag
  private implicit def order = index.order

  def size: Int = index.size

  /** Returns this series as a collection of key/value pairs. */
  def to[CC[_]](implicit cbf: CanBuildFrom[Nothing, (K, Cell[V]), CC[(K, Cell[V])]]): CC[(K, Cell[V])] = {
    val bldr = cbf()
    bldr.sizeHint(size)
    iterator.foreach { bldr += _ }
    bldr.result()
  }

  /** Returns an iterator over the key-cell pairs of the series.
    * @return an iterator over the key-cell pairs of the series.
    * @see [[denseIterator]]
    * @note If the series is known to be dense, or the non values
    * can ignored, then one should use [[denseIterator]] instead.
    */
  def iterator: Iterator[(K, Cell[V])] = index.iterator map { case (k, row) =>
    k -> column(row)
  }

  /** Returns an iterator over the key-value pairs of the series.
    *
    * This iterator assumes that the series is dense, so it will
    * skip over any non value cells if, in fact, the series is sparse.
    *
    * @return an iterator over the key-value pairs of the series.
    * @see [[iterator]]
    * @note The `Iterator` returned by this method is related to
    * the `Iterator` returned by [[iterator]]
    * {{{
    * series.iterator.collect { case (k, Value(v)) => k -> v} == series.denseIterator
    * }}}
    * however, this method uses a more efficient access pattern
    * to the underlying data.
    */
  def denseIterator: Iterator[(K, V)] =
    index.iterator collect { case (k, ix) if column.isValueAt(ix) =>
      k -> column.valueAt(ix)
    }

  def keys: Vector[K] = index.map(_._1)(collection.breakOut)
  def values: Vector[Cell[V]] = index.map({ case (_, i) => column(i) })(collection.breakOut)

  def keyAt(i: Int): K = index.keyAt(i)
  def valueAt(i: Int): Cell[V] = column(index.indexAt(i))

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA

  /**
   * Returns `true` if at least 1 value exists in this series. A series with
   * only `NA`s and/or `NM`s will return `false`.
   */
  def hasValues: Boolean = {
    var i = 0
    var seenValue = false
    while (i < index.size && !seenValue) {
      val row = index.indexAt(i)
      seenValue = seenValue & column.isValueAt(row)
      i += 1
    }
    seenValue
  }

  /**
   * Merges 2 series together using a semigroup to append values.
   */
  def merge[VV >: V: Semigroup: ClassTag](that: Series[K, VV]): Series[K, VV] = {
    val merger = Merger[K](Merge.Outer)
    val (keys, lIndices, rIndices) = Index.cogroup(this.index, that.index)(merger).result()
    val lCol = this.column
    val rCol = that.column

    // TODO: Remove duplication between this and zipMap.
    val bldr = Column.builder[VV]
    cfor(0)(_ < lIndices.length, _ + 1) { i =>
      val l = lIndices(i)
      val r = rIndices(i)
      val lExists = lCol.isValueAt(l)
      val rExists = rCol.isValueAt(r)
      if (lExists && rExists) {
        bldr.addValue((lCol.valueAt(l): VV) |+| rCol.valueAt(r))
      } else if (lExists) {
        if (rCol.nonValueAt(r) == NM) bldr.addNM()
        else bldr.addValue(lCol.valueAt(l))
      } else if (rExists) {
        if (lCol.nonValueAt(l) == NM) bldr.addNM()
        else bldr.addValue(rCol.valueAt(r))
      } else {
        bldr.addNonValue(if (lCol.nonValueAt(l) == NM) NM else rCol.nonValueAt(r))
      }
    }

    Series(Index.ordered(keys), bldr.result())
  }

  /**
   * Merges 2 series together, taking the first non-NA or NM value.
   */
  def ++[VV >: V: ClassTag](that: Series[K, VV]): Series[K, VV] = {
    val merger = Merger[K](Merge.Outer)
    val (keys, lIndices, rIndices) = Index.cogroup(this.index, that.index)(merger).result()
    val lCol = this.column
    val rCol = that.column

    // TODO: Remove duplication between this and zipMap.
    val bldr = Column.builder[VV]
    cfor(0)(_ < lIndices.length, _ + 1) { i =>
      val l = lIndices(i)
      val r = rIndices(i)
      val lExists = lCol.isValueAt(l)
      val rExists = rCol.isValueAt(r)
      if (lExists && rExists) {
        bldr.addValue(lCol.valueAt(l): VV)
      } else if (lExists) {
        bldr.addValue(lCol.valueAt(l))
      } else if (rExists) {
        bldr.addValue(rCol.valueAt(r))
      } else {
        bldr.addNonValue(if (lCol.nonValueAt(l) == NM) NM else rCol.nonValueAt(r))
      }
    }

    Series(Index.ordered(keys), bldr.result())
  }

  /**
   * Perform an inner join with `that` and group the values in tuples.
   *
   * Equivalent to calling `lhs.zipMap(rhs)((_, _))`.
   */
  def zip[W](that: Series[K, (V, W)]): Series[K, (V, W)] =
    zipMap(that)((_, _))

  /**
   * Performs an inner join on this `Series` with `that`. Each pair of values
   * for a matching key is passed to `f`.
   */
  def zipMap[W, X: ClassTag](that: Series[K, W])(f: (V, W) => X): Series[K, X] = {
    val joiner = Joiner[K](Join.Inner)
    val (keys, lIndices, rIndices) = Index.cogroup(this.index, that.index)(joiner).result()
    val bldr = Column.builder[X]

    val lCol = this.column
    val rCol = that.column
    cfor(0)(_ < lIndices.length, _ + 1) { i =>
      val l = lIndices(i)
      val r = rIndices(i)
      val lExists = lCol.isValueAt(l)
      val rExists = rCol.isValueAt(r)
      if (lExists && rExists) {
        bldr.addValue(f(lCol.valueAt(l), rCol.valueAt(r)))
      } else if (lExists) {
        bldr.addNonValue(rCol.nonValueAt(r))
      } else if (rExists) {
        bldr.addNonValue(lCol.nonValueAt(l))
      } else {
        bldr.addNonValue(if (lCol.nonValueAt(l) == NM) NM else rCol.nonValueAt(r))
      }
    }
    Series(Index.ordered(keys), bldr.result())
  }

  /**
   * Sort this series by index keys and return it. The sort should be stable,
   * so the relative order within a key will remain the same.
   */
  def sorted: Series[K, V] = Series(index.sorted, column)

  /**
   * Convert this Series to a single column [[Frame]].
   */
  def toFrame[C: Order: ClassTag](col: C)(implicit tt: ClassTag[V]): Frame[K, C] =
    Frame(index, col -> TypedColumn(column))

  def toFrame(implicit tt: ClassTag[V]): Frame[K, Int] = {
    import spire.std.int._
    Frame[K, Int](index, 0 -> TypedColumn(column))
  }

  def closestKeyTo(k: K, tolerance: Double)(implicit K0: MetricSpace[K, Double], K1: Order[K]): Option[K] =
    apply(k) match {
      case Value(v) => Some(k)
      case _ =>
        val possibleDates =
          keys.filter { key =>
            val distance = K0.distance(key, k)
            (distance <= tolerance)
          }
        possibleDates.headOption
    }

  /**
   * Map the keys of this series. This will maintain the same iteration order
   * as the old series.
   */
  def mapKeys[L: Order: ClassTag](f: K => L): Series[L, V] =
    Series(index.map { case (k, i) => f(k) -> i }, column)

  /**
   * Map the values of this series only. Note that the function `f` will be
   * called every time a value is accessed. To prevent this, you must `compact`
   * the Series.
   */
  def mapValues[W](f: V => W): Series[K, W] =
    Series(index, new MappedColumn(f, column)) // TODO: Use a macro here?

  /**
   * Filter the this series by its keys.
   */
  def filterKeys(p: K => Boolean): Series[K, V] = {
    val b = new SeriesBuilder[K, V]
    b.sizeHint(this.size)
    for ((k, ix) <- index.iterator) {
      if (p(k)) {
        b += (k -> column(ix))
      }
    }
    b.result()
  }

  /**
   * Filter the values of this series only.
   */
  def filterValues(p: Cell[V] => Boolean): Series[K, V] = {
    val b = new SeriesBuilder[K, V]
    b.sizeHint(this.size)
    for ((k, ix) <- index.iterator) {
      val cell = column(ix)
      if (p(cell)) {
        b += (k -> cell)
      }
    }
    b.result()
  }

  def firstValue: Option[(K, V)] = {
    var i = 0
    while (i < index.size) {
      val row = index.indexAt(i)
      if (column.isValueAt(row))
        return Some(index.keyAt(i) -> column.valueAt(row))
      i += 1
    }
    None
  }

  def lastValue: Option[(K, V)] = {
    var i = index.size - 1
    while (i >= 0) {
      val row = index.indexAt(i)
      if (column.isValueAt(row))
        return Some(index.keyAt(i) -> column.valueAt(row))
      i -= 1
    }
    None
  }

  /**
   * Returns a compacted version of this `Series`. The new series will be equal
   * to the old one, but the backing column will be dropped and replaced with a
   * version that only contains the values needed for this series. It will also
   * remove any indirection in the underlying column, such as that caused by
   * reindexing, shifting, mapping values, etc.
   */
  def compacted[V: ClassTag]: Series[K, V] = ???

  /**
   * Reduce all the values in this `Series` using the given reducer.
   */
  def reduce[W](reducer: Reducer[V, W]): Cell[W] = {
    val indices = new Array[Int](index.size)
    cfor(0)(_ < indices.length, _ + 1) { i =>
      indices(i) = index.indexAt(i)
    }
    reducer.reduce(column, indices, 0, index.size)
  }

  /** Returns the [[reduce.Count]] reduction of this series.
    * @return the [[reduce.Count]] reduction of this series.
    * @see [[reduce.Count]]
    */
  def count: Cell[Int] =
    this.reduce(framian.reduce.Count)

  /** Returns the [[reduce.First]] reduction of this series.
    * @return the [[reduce.First]] reduction of this series.
    * @see [[reduce.First]]
    * @see [[firstN]]
    * @see [[last]]
    */
  def first: Cell[V] =
    this.reduce(framian.reduce.First[V])

  /** Returns the [[reduce.FirstN]] reduction of this series.
    * @return the [[reduce.FirstN]] reduction of this series.
    * @see [[reduce.FirstN]]
    * @see [[first]]
    * @see [[lastN]]
    */
  def firstN(n: Int): Cell[List[V]] =
    this.reduce(framian.reduce.FirstN[V](n))

  /** Returns the [[reduce.Last]] reduction of this series.
    * @return the [[reduce.Last]] reduction of this series.
    * @see [[reduce.Last]]
    * @see [[lastN]]
    * @see [[first]]
    */
  def last: Cell[V] =
    this.reduce(framian.reduce.Last[V])

  /** Returns the [[reduce.LastN]] reduction of this series.
    * @return the [[reduce.LastN]] reduction of this series.
    * @see [[reduce.LastN]]
    * @see [[last]]
    * @see [[firstN]]
    */
  def lastN(n: Int): Cell[List[V]] =
    this.reduce(framian.reduce.LastN[V](n))

  /** Returns the [[reduce.Max]] reduction of this series.
    * @return the [[reduce.Max]] reduction of this series.
    * @see [[reduce.Max]]
    * @see [[min]]
    */
  def max(implicit ev0: Order[V]): Cell[V] =
    this.reduce(framian.reduce.Max[V])

  /** Returns the [[reduce.Min]] reduction of this series.
    * @return the [[reduce.Min]] reduction of this series.
    * @see [[reduce.Min]]
    * @see [[max]]
    */
  def min(implicit ev0: Order[V]): Cell[V] =
    this.reduce(framian.reduce.Min[V])

  /** Returns the `AdditiveMonoid` reduction of this series.
    * @return the `AdditiveMonoid` reduction of this series.
    * @see [[reduce.MonoidReducer]]
    * @see [[sumNonEmpty]]
    * @see [[product]]
    */
  def sum(implicit ev0: AdditiveMonoid[V]): Cell[V] =
    this.reduce(framian.reduce.MonoidReducer[V](ev0.additive))

  /** Returns the `AdditiveSemigroup` reduction of this series.
    * @return the `AdditiveSemigroup` reduction of this series.
    * @see [[reduce.SemigroupReducer]]
    * @see [[sum]]
    * @see [[productNonEmpty]]
    */
  def sumNonEmpty(implicit ev0: AdditiveSemigroup[V]): Cell[V] =
    this.reduce(framian.reduce.SemigroupReducer[V](ev0.additive))

  /** Returns the `MultiplicativeMonoid` reduction of this series.
    * @return the `MultiplicativeMonoid` reduction of this series.
    * @see [[reduce.MonoidReducer]]
    * @see [[productNonEmpty]]
    * @see [[sum]]
    */
  def product(implicit ev0: MultiplicativeMonoid[V]): Cell[V] =
    this.reduce(framian.reduce.MonoidReducer[V](ev0.multiplicative))

  /** Returns the `MultiplicativeSemigroup` reduction of this series.
    * @return the `MultiplicativeSemigroup` reduction of this series.
    * @see [[reduce.SemigroupReducer]]
    * @see [[product]]
    * @see [[sumNonEmpty]]
    */
  def productNonEmpty(implicit ev0: MultiplicativeSemigroup[V]): Cell[V] =
    this.reduce(framian.reduce.SemigroupReducer[V](ev0.multiplicative))

  /** Returns the [[reduce.Mean]] reduction of this series.
    * @return the [[reduce.Mean]] reduction of this series.
    * @see [[reduce.Mean]]
    * @see [[median]]
    */
  def mean(implicit ev0: Field[V]): Cell[V] =
    this.reduce(framian.reduce.Mean[V])

  /** Returns the [[reduce.Median]] reduction of this series.
    * @return the [[reduce.Median]] reduction of this series.
    * @see [[reduce.Median]]
    * @see [[mean]]
    */
  def median(implicit ev0: ClassTag[V], ev1: Field[V], ev2: Order[V]): Cell[V] =
    this.reduce(framian.reduce.Median[V])

  /** Returns the [[reduce.Unique]] reduction of this series.
    * @return the [[reduce.Unique]] reduction of this series.
    * @see [[reduce.Unique]]
    */
  def unique: Cell[Set[V]] =
    this.reduce(framian.reduce.Unique[V])

  /** Returns the [[reduce.Exists]] reduction of this series.
    * @return the [[reduce.Exists]] reduction of this series.
    * @see [[reduce.Exists]]
    * @see [[forall]]
    */
  def exists(p: V => Boolean): Boolean = {
    val cell = this.reduce(framian.reduce.Exists(p))
    assume(cell.isValue, "assumed that the Exists reducer always returns a value")
    cell.get
  }

  /** Returns the [[reduce.ForAll]] reduction of this series.
    * @return the [[reduce.ForAll]] reduction of this series.
    * @see [[reduce.ForAll]]
    * @see [[exists]]
    */
  def forall(p: V => Boolean): Boolean = {
    val cell = this.reduce(framian.reduce.ForAll(p))
    assume(cell.isValue, "assumed that the ForAll reducer always returns a value")
    cell.get
  }

  /**
   * For each unique key in this series, this reduces all the values for that
   * key and returns a series with only the unique keys and reduced values. The
   * new series will be in key order.
   */
  def reduceByKey[W](reducer: Reducer[V, W]): Series[K, W] = {
    val reduction = new Reduction[K, V, W](column, reducer)
    val (keys, values) = Index.group(index)(reduction).result()
    Series(Index.ordered(keys), Column.fromCells(values))
  }

  /** Returns the [[reduce.Count]] reduction of this series by key.
    * @return the [[reduce.Count]] reduction of this series by key.
    * @see [[reduce.Count]]
    */
  def countByKey: Series[K, Int] =
    this.reduceByKey(framian.reduce.Count)

  /** Returns the [[reduce.First]] reduction of this series by key.
    * @return the [[reduce.First]] reduction of this series by key.
    * @see [[reduce.First]]
    * @see [[firstN]]
    * @see [[last]]
    */
  def firstByKey: Series[K, V] =
    this.reduceByKey(framian.reduce.First[V])

  /** Returns the [[reduce.FirstN]] reduction of this series by key.
    * @return the [[reduce.FirstN]] reduction of this series by key.
    * @see [[reduce.FirstN]]
    * @see [[first]]
    * @see [[lastN]]
    */
  def firstNByKey(n: Int): Series[K, List[V]] =
    this.reduceByKey(framian.reduce.FirstN[V](n))

  /** Returns the [[reduce.Last]] reduction of this series by key.
    * @return the [[reduce.Last]] reduction of this series by key.
    * @see [[reduce.Last]]
    * @see [[lastN]]
    * @see [[first]]
    */
  def lastByKey: Series[K, V] =
    this.reduceByKey(framian.reduce.Last[V])

  /** Returns the [[reduce.LastN]] reduction of this series by key.
    * @return the [[reduce.LastN]] reduction of this series by key.
    * @see [[reduce.LastN]]
    * @see [[last]]
    * @see [[firstN]]
    */
  def lastNByKey(n: Int): Series[K, List[V]] =
    this.reduceByKey(framian.reduce.LastN[V](n))

  /** Returns the [[reduce.Max]] reduction of this series by key.
    * @return the [[reduce.Max]] reduction of this series by key.
    * @see [[reduce.Max]]
    * @see [[min]]
    */
  def maxByKey(implicit ev0: Order[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.Max[V])

  /** Returns the [[reduce.Min]] reduction of this series by key.
    * @return the [[reduce.Min]] reduction of this series by key.
    * @see [[reduce.Min]]
    * @see [[max]]
    */
  def minByKey(implicit ev0: Order[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.Min[V])

  /** Returns the `AdditiveMonoid` reduction of this series by key.
    * @return the `AdditiveMonoid` reduction of this series by key.
    * @see [[reduce.MonoidReducer]]
    * @see [[sumNonEmpty]]
    * @see [[product]]
    */
  def sumByKey(implicit ev0: AdditiveMonoid[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.MonoidReducer[V](ev0.additive))

  /** Returns the `AdditiveSemigroup` reduction of this series by key.
    * @return the `AdditiveSemigroup` reduction of this series by key.
    * @see [[reduce.SemigroupReducer]]
    * @see [[sum]]
    * @see [[productNonEmpty]]
    */
  def sumNonEmptyByKey(implicit ev0: AdditiveSemigroup[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.SemigroupReducer[V](ev0.additive))

  /** Returns the `MultiplicativeMonoid` reduction of this series by key.
    * @return the `MultiplicativeMonoid` reduction of this series by key.
    * @see [[reduce.MonoidReducer]]
    * @see [[productNonEmpty]]
    * @see [[sum]]
    */
  def productByKey(implicit ev0: MultiplicativeMonoid[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.MonoidReducer[V](ev0.multiplicative))

  /** Returns the `MultiplicativeSemigroup` reduction of this series by key.
    * @return the `MultiplicativeSemigroup` reduction of this series by key.
    * @see [[reduce.SemigroupReducer]]
    * @see [[product]]
    * @see [[sumNonEmpty]]
    */
  def productNonEmptyByKey(implicit ev0: MultiplicativeSemigroup[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.SemigroupReducer[V](ev0.multiplicative))

  /** Returns the [[reduce.Mean]] reduction of this series by key.
    * @return the [[reduce.Mean]] reduction of this series by key.
    * @see [[reduce.Mean]]
    * @see [[median]]
    */
  def meanByKey(implicit ev0: Field[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.Mean[V])

  /** Returns the [[reduce.Median]] reduction of this series by key.
    * @return the [[reduce.Median]] reduction of this series by key.
    * @see [[reduce.Median]]
    * @see [[mean]]
    */
  def medianByKey(implicit ev0: ClassTag[V], ev1: Field[V], ev2: Order[V]): Series[K, V] =
    this.reduceByKey(framian.reduce.Median[V])

  /** Returns the [[reduce.Unique]] reduction of this series by key.
    * @return the [[reduce.Unique]] reduction of this series by key.
    * @see [[reduce.Unique]]
    */
  def uniqueByKey: Series[K, Set[V]] =
    this.reduceByKey(framian.reduce.Unique[V])

  /** Returns the [[reduce.Exists]] reduction of this series by key.
    * @return the [[reduce.Exists]] reduction of this series by key.
    * @see [[reduce.Exists]]
    * @see [[forall]]
    */
  def existsByKey(p: V => Boolean): Series[K, Boolean] =
    this.reduceByKey(framian.reduce.Exists(p))

  /** Returns the [[reduce.ForAll]] reduction of this series by key.
    * @return the [[reduce.ForAll]] reduction of this series by key.
    * @see [[reduce.ForAll]]
    * @see [[exists]]
    */
  def forallByKey(p: V => Boolean): Series[K, Boolean] =
    this.reduceByKey(framian.reduce.ForAll(p))


  /**
   * Rolls values and `NM`s forward, over `NA`s. This is similar to
   * [[rollForwardUpTo]], but has no bounds checks. In fact, this is exactly
   * equivalent to
   * `series.rollForwardUpTo(1)(TrivialMetricSpace[K], Order[Int])`.
   */
  def rollForward: Series[K, V] =
    rollForwardUpTo[Int](1)(TrivialMetricSpace[K], Order[Int])

  /**
   * Roll-forward values and `NM`s over `NA`s. It will rolls values in sequence
   * order (not sorted order). It will only roll over `NA`s whose key is within
   * `delta` of the last valid value or `NM`. This bounds check is inclusive.
   *
   * An example of this behaviour is as follows:
   * {{{
   * Series(1 -> "a", 2 -> NA, 3 -> NA, 4 -> NM, 5 -> NA, 6 -> NA).rollForwardUpTo(1D) ===
   *     Series(1 -> "a", 2 -> "a", 3 -> NA, 4 -> NM, 5 -> NM, 6 -> NA)
   * }}}
   */
  def rollForwardUpTo[R](delta: R)(implicit K: MetricSpace[K, R], R: Order[R]): Series[K, V] = {
    val indices: Array[Int] = new Array[Int](index.size)

    @tailrec
    def loop(i: Int, lastValue: Int): Unit = if (i < index.size) {
      val row = index.indexAt(i)
      if (!column.isValueAt(row) && column.nonValueAt(row) == NA) {
        if (K.distance(index.keyAt(i), index.keyAt(lastValue)) <= delta) {
          indices(i) = index.indexAt(lastValue)
        } else {
          indices(i) = row
        }
        loop(i + 1, lastValue)
      } else {
        indices(i) = row
        loop(i + 1, i)
      }
    }

    loop(0, 0)

    Series(index, column.reindex(indices))
  }

  override def toString: String =
    (keys zip values).map { case (key, value) =>
      s"$key -> $value"
    }.mkString("Series(", ", ", ")")

  override def equals(that0: Any): Boolean = that0 match {
    case (that: Series[_, _]) if this.index.size == that.index.size =>
      (this.index.iterator zip that.index.iterator) forall {
        case ((key0, idx0), (key1, idx1)) if key0 == key1 =>
          this.column(idx0) == that.column(idx1)
        case _ => false
      }
    case _ => false
  }

  override def hashCode: Int =
    index.map { case (k, i) => (k, column(i)) }.hashCode
}

object Series {
  import spire.std.int._
  def empty[K: Order: ClassTag, V] = Series(Index.empty[K], Column.empty[V])

  def apply[K, V](index: Index[K], column: Column[V]): Series[K, V] =
    new Series(index, column)

  def apply[K: Order: ClassTag, V: ClassTag](kvs: (K, V)*): Series[K, V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column.fromArray(values.toArray))
  }

  def apply[V: ClassTag](values: V*): Series[Int, V] = {
    val keys = Array(0 to (values.length - 1): _*)
    Series(Index(keys), Column.fromArray(values.toArray))
  }

  def fromCells[K: Order: ClassTag, V: ClassTag](col: TraversableOnce[(K, Cell[V])]): Series[K, V] = {
    val bldr = new SeriesBuilder[K ,V]
    bldr ++= col
    bldr.result()
  }

  def fromCells[K: Order: ClassTag, V: ClassTag](kvs: (K, Cell[V])*): Series[K, V] =
    fromCells(kvs)

  def fromMap[K: Order: ClassTag, V: ClassTag](kvMap: Map[K, V]): Series[K, V] =
    Series(Index(kvMap.keys.toArray), Column.fromArray(kvMap.values.toArray))

  implicit def cbf[K: Order: ClassTag, V]: CanBuildFrom[Series[_, _], (K, Cell[V]), Series[K, V]] =
    new CanBuildFrom[Series[_, _], (K, Cell[V]), Series[K, V]] {
      def apply(): Builder[(K, Cell[V]), Series[K, V]] = new SeriesBuilder[K, V]
      def apply(from: Series[_, _]): Builder[(K, Cell[V]), Series[K, V]] = apply()
    }
}

private final class SeriesBuilder[K: Order: ClassTag, V] extends Builder[(K, Cell[V]), Series[K,V]] {
  val keys = ArrayBuilder.make[K]()
  val values = ArrayBuilder.make[Cell[V]]()

  def +=(elem: (K, Cell[V])) = {
    keys += elem._1
    values += elem._2
    this
  }

  def clear(): Unit = {
    keys.clear()
    values.clear()
  }

  def result(): Series[K, V] = Series(Index.unordered(keys.result()), Column.fromCells(values.result()))

  override def sizeHint(size: Int): Unit = {
    keys.sizeHint(size)
    values.sizeHint(size)
  }
}
