package pellucid
package pframe

import scala.reflect.ClassTag

import spire.algebra._
// import spire.std.option._
import spire.syntax.additiveMonoid._
import spire.syntax.monoid._
import spire.syntax.cfor._

case class Series[K,V](index: Index[K], column: Column[V]) {
  def keys: Vector[K] = index.keys.toVector
  def values: Vector[Cell[V]] = (0 until index.size).map(column(_)).toVector

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA

  def mapValues[W](f: V => W): Series[K, W] =
    Series(index, column map f)

  def reduce(implicit V: Monoid[V]): V = {
    val indices = index.indices
    var sum: V = V.id
    cfor(0)(_ < indices.length, _ + 1) { i =>
      val row = indices(i)
      if (column.exists(row))
        sum = sum |+| column.value(row)
    }
    sum
  }

  // def reduceOption(implicit V: Semigroup[V]): Option[V] =
  //   this.mapValues[Option[V]](Some(_)).reduce

  def sum(implicit V: AdditiveMonoid[V]): V =
    reduce(V.additive)

  override def toString: String =
    (keys zip values).map { case (key, value) =>
      s"$key -> $value"
    }.mkString("Series(", ", ", ")")
}

object Series {
  def apply[K: Order: ClassTag, V: ClassTag](kvs: (K, V)*): Series[K,V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column.fromArray(values.toArray))
  }
}

