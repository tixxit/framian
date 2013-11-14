package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{ TypeTag, typeTag }

import spire.algebra._
import spire.syntax.additiveMonoid._
import spire.syntax.cfor._

case class Series[K,V](index: Index[K], column: Column[V]) {
  def keys: Vector[K] = index.keys.toVector
  def values: Vector[Cell[V]] = (0 until index.size).map(column(_)).toVector

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA

  def sum(implicit V: AdditiveMonoid[V]): V = {
    val indices = index.indices
    var sum: V = V.zero
    cfor(0)(_ < indices.length, _ + 1) { i =>
      val row = indices(i)
      if (column.exists(row))
        sum += column.value(row)
    }
    sum
  }
}

object Series {
  def apply[K: Order: ClassTag, V: TypeTag: ClassTag](kvs: (K, V)*): Series[K,V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column(values.toArray))
  }
}

