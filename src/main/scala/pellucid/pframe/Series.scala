package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{ TypeTag, typeTag }

import spire.algebra.Order

case class Series[K,V](index: Index[K], column: Column[V]) {
  def keys: Vector[K] = index.keys.toVector
  def values: Vector[Cell[V]] = (0 until index.size).map(column(_)).toVector

  def apply(key: K): Cell[V] = index.get(key) map (column(_)) getOrElse NA
}

object Series {
  def apply[K: Order: ClassTag, V: TypeTag: ClassTag](kvs: (K, V)*): Series[K,V] = {
    val (keys, values) = kvs.unzip
    Series(Index(keys.toArray), Column(values.toArray))
  }
}

