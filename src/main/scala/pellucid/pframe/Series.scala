package pellucid
package pframe

import scala.annotation.tailrec
import scala.collection.mutable.{ ArrayBuilder, Builder }
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
// import spire.std.option._
import spire.syntax.additiveMonoid._
import spire.syntax.monoid._
import spire.syntax.cfor._

final class Series[K,V](val index: Index[K], val column: Column[V]) {
  private implicit def classTag = index.classTag
  private implicit def order = index.order

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

  def sorted: Series[K, V] = Series(index.sorted, column)

  def toFrame[C: Order: ClassTag](col: C)(implicit tt: TypeTag[V]): Frame[K, C] =
    Frame(index, col -> TypedColumn(column))

  def mapValues[W](f: V => W): Series[K, W] =
    Series(index, column map f)

  def reduceByKey[W: ClassTag](reducer: Reducer[V, W]): Series[K, W] = {
    val reduction = new Reduction[K, V, W](column, reducer)
    val (keys, values) = Index.group(index)(reduction).result()
    Series(Index.ordered(keys), Column.fromArray(values))
  }

  def reduce[W: ClassTag](reducer: Reducer[V, W]): W = {
    val indices = new Array[Int](index.size)
    cfor(0)(_ < indices.length, _ + 1) { i =>
      indices(i) = index.indexAt(i)
    }
    reducer.reduce(column, indices, 0, index.size)
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
  def empty[K: Order: ClassTag, V] = Series(Index.empty[K], Column.empty[V])

  def apply[K, V](index: Index[K], column: Column[V]): Series[K, V] =
    new Series(index, column)

  def apply[K: Order: ClassTag, V: ClassTag](kvs: (K, V)*): Series[K,V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column.fromArray(values.toArray))
  }
}
