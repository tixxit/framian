package pellucid
package pframe

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ ArrayBuilder, Builder }
import scala.collection.{ IterableLike, Iterable }
import scala.reflect.ClassTag

import spire.algebra._
import spire.math._
import spire.std.any._
// import spire.std.option._
import spire.syntax.additiveMonoid._
import spire.syntax.monoid._
import spire.syntax.cfor._

import pellucid.pframe.reduce.Reducer

final class Series[K,V](val index: Index[K], val column: Column[V])
    extends Iterable[(K, Cell[V])] with IterableLike[(K, Cell[V]), Series[K, V]] {

  private implicit def classTag = index.classTag
  private implicit def order = index.order

  def empty: Series[K, V] = Series(Index.empty, Column.empty)
  override def size: Int = index.size
  def iterator: Iterator[(K, Cell[V])] = index.iterator map { case (k, row) =>
    k -> column(row)
  }
  override protected def newBuilder: Builder[(K, Cell[V]), Series[K, V]] =
    new SeriesBuilder

  def keys: Vector[K] = index.map(_._1)(collection.breakOut)
  def values: Vector[Cell[V]] = index.map({ case (_, i) => column(i) })(collection.breakOut)

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA

  /**
   * Performs an inner join on this `Series` with `that`. Each pair of values
   * for a matching key is passed to `f`.
   */
  def zipMap[W, X: ClassTag](that: Series[K, W])(f: (V, W) => X): Series[K, X] = {
    val joiner = Joiner[K](Join.Inner)
    val (keys, lIndices, rIndices) = Index.cogroup(this.index, that.index)(joiner).result()
    val values = new Array[X](keys.length)
    val lCol = this.column
    val rCol = that.column
    cfor(0)(_ < keys.length, _ + 1) { i =>
      val key = keys(i)
      values(i) = f(lCol.value(lIndices(i)), rCol.value(rIndices(i)))
    }
    Series(Index.ordered(keys), Column.fromArray(values))
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

  /**
   * Map the values of this series only. Note that the function `f` will be
   * called every time a value is accessed. To prevent this, you must `compact`
   * the Series.
   */
  def mapValues[W](f: V => W): Series[K, W] =
    Series(index, new MappedColumn(f, column)) // TODO: Use a macro here?

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

  def reindex(newIndex: Index[K]) = this.copy(newIndex, column)

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
  def empty[K: Order: ClassTag, V] = Series(Index.empty[K], Column.empty[V])

  def apply[K, V](index: Index[K], column: Column[V]): Series[K, V] =
    new Series(index, column)

  def apply[K: Order: ClassTag, V: ClassTag](kvs: (K, V)*): Series[K,V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column.fromArray(values.toArray))
  }

  def apply[V: ClassTag](values: V*): Series[Int,V] = {
    val keys = Array(0 to (values.length - 1): _*)
    Series(Index(keys), Column.fromArray(values.toArray))
  }

  implicit def cbf[K: Order: ClassTag, V]: CanBuildFrom[Series[_, _], (K, Cell[V]), Series[K, V]] =
    new CanBuildFrom[Series[_, _], (K, Cell[V]), Series[K, V]] {
      def apply(): Builder[(K, Cell[V]), Series[K, V]] = new SeriesBuilder[K, V]
      def apply(from: Series[_, _]): Builder[(K, Cell[V]), Series[K, V]] = apply()
    }
}

private final class SeriesBuilder[K: Order: ClassTag, V] extends Builder[(K, Cell[V]), Series[K, V]] {
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
