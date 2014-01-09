package pellucid
package pframe

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
import spire.compat._
import spire.syntax.cfor._

import pellucid.pframe.reduce.Reducer
import pellucid.pframe.util.TrivialMetricSpace

final class Series[K,V](val index: Index[K], val column: Column[V])
    extends Iterable[(K, Cell[V])] with IterableLike[(K, Cell[V]), Series[K, V]] {

  private implicit def classTag = index.classTag
  private implicit def order = index.order

  def empty: Series[K, V] = Series(Index.empty[K], Column.empty[V])
  override def size: Int = index.size
  def iterator: Iterator[(K, Cell[V])] = index.iterator map { case (k, row) =>
    k -> column(row)
  }
  override protected def newBuilder: Builder[(K, Cell[V]), Series[K, V]] =
    new SeriesBuilder

  def keys: Vector[K] = index.map(_._1)(collection.breakOut)
  def values: Vector[Cell[V]] = index.map({ case (_, i) => column(i) })(collection.breakOut)

  def keyAt(i: Int): K = index.keyAt(i)
  def valueAt(i: Int): Cell[V] = column(index.indexAt(i))

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA

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
    cfor(0)(_ < keys.length, _ + 1) { i =>
      val key = keys(i)
      val l = lIndices(i)
      val r = rIndices(i)
      val lExists = lCol.exists(l)
      val rExists = rCol.exists(r)
      if (lExists && rExists) {
        bldr.addValue(f(lCol.value(l), rCol.value(r)))
      } else if (lExists) {
        bldr.addMissing(rCol.missing(r))
      } else if (rExists) {
        bldr.addMissing(lCol.missing(l))
      } else {
        bldr.addMissing(if (lCol.missing(l) == NM) NM else rCol.missing(r))
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

  /**
   * Map the values of this series only. Note that the function `f` will be
   * called every time a value is accessed. To prevent this, you must `compact`
   * the Series.
   */
  def mapValues[W](f: V => W): Series[K, W] =
    Series(index, new MappedColumn(f, column)) // TODO: Use a macro here?

  /**
   * Filter the values of this series only.
   */
  def filterValues(f: Cell[V] => Boolean): Series[K, V] = this.filter { case (index, value) => f(value) }

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
  def reduce[W: ClassTag](reducer: Reducer[V, W]): W = {
    val indices = new Array[Int](index.size)
    cfor(0)(_ < indices.length, _ + 1) { i =>
      indices(i) = index.indexAt(i)
    }
    reducer.reduce(column, indices, 0, index.size)
  }

  /**
   * For each unique key in this series, this reduces all the values for that
   * key and returns a series with only the unique keys and reduced values. The
   * new series will be in key order.
   */
  def reduceByKey[W: ClassTag](reducer: Reducer[V, W]): Series[K, W] = {
    val reduction = new Reduction[K, V, W](column, reducer)
    val (keys, values) = Index.group(index)(reduction).result()
    Series(Index.ordered(keys), Column.fromArray(values))
  }

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
      if (!column.exists(row) && column.missing(row) == NA) {
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

    Series(index.withIndices(indices), column)
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

  def fromCells[K: Order: ClassTag, V: ClassTag](kvs: (K, Cell[V])*): Series[K, V] = {
    val bldr = new SeriesBuilder[K ,V]
    bldr ++= kvs
    bldr.result()
  }

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
}
