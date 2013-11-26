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

case class Series[K,V](index: Index[K], column: Column[V]) {
  private implicit def classTag = index.classTag
  private implicit def order = index.order

  def keys: Vector[K] = index.map(_._1)(collection.breakOut)
  def values: Vector[Cell[V]] = index.map({ case (_, i) => column(i) })(collection.breakOut)

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA

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

  def reduce[W: ClassTag](reducer: Reducer[V, W]): W =
    reducer.reduce(column, index.indices, 0, index.size) // TODO: This is incorrect. Should be index.ord.

  // def reduceOption(implicit V: Semigroup[V]): Option[V] =
  //   this.mapValues[Option[V]](Some(_)).reduce

  def sum(implicit V: AdditiveMonoid[V], ct: ClassTag[V]): V =
    reduce(Reducers.monoid(V.additive))

  def sumByKey(implicit V: AdditiveMonoid[V], ct: ClassTag[V]): Series[K, V] =
    reduceByKey(Reducers.monoid(V.additive))

  override def toString: String =
    (keys zip values).map { case (key, value) =>
      s"$key -> $value"
    }.mkString("Series(", ", ", ")")
}

object Series {
  def empty[K: Order: ClassTag, V] = Series(Index.empty[K], Column.empty[V])

  def apply[K: Order: ClassTag, V: ClassTag](kvs: (K, V)*): Series[K,V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column.fromArray(values.toArray))
  }
}
