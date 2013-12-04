package pellucid
package pframe

import scala.annotation.tailrec
import scala.collection.mutable.{ ArrayBuilder, Builder }
import scala.reflect.ClassTag

import spire.algebra._
import spire.math._
import spire.std.any._
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

  def mapValues[W](f: V => W): Series[K, W] =
    Series(index, column map f)

  def reduceByKey(implicit V: Monoid[V], ct: ClassTag[V]): Series[K, V] = {
    val reduction = new Reduction[K, V](column)
    val (keys, values) = Index.group(index)(reduction).result()
    Series(Index.ordered(keys), Column.fromArray(values))
  }

  def reduce(implicit V: Monoid[V]): V = {
    var sum: V = V.id // TODO: Closing over a var.
    index.foreach { (_, row) =>
      if (column.exists(row))
        sum = sum |+| column.value(row)
    }
    sum
  }

  // def reduceOption(implicit V: Semigroup[V]): Option[V] =
  //   this.mapValues[Option[V]](Some(_)).reduce

  def sum(implicit V: AdditiveMonoid[V]): V =
    reduce(V.additive)

  def sumByKey(implicit V: AdditiveMonoid[V], ct: ClassTag[V]): Series[K, V] =
    reduceByKey(V.additive, ct)

  def reindex(newIndex: Index[K]) = this.copy(newIndex, column)

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

  def apply[V: ClassTag](values: V*): Series[Int,V] = {
    val keys = Array(0 to (values.length - 1): _*)
    Series(Index(keys), Column.fromArray(values.toArray))
  }
}

private final class Reduction[K: ClassTag, V: Monoid: ClassTag](column: Column[V]) extends Index.Grouper[K] {
  final class State {
    val keys = ArrayBuilder.make[K]
    val values = ArrayBuilder.make[V]

    def add(key: K, value: V) {
      keys += key
      values += value
    }

    def result() = (keys.result(), values.result())
  }

  def init = new State

  def group(state: State)(keys: Array[K], indices: Array[Int], start: Int, end: Int): State = {
    @tailrec def reduce(i: Int, acc: V): V = if (i < end) {
      val row = indices(i)
      reduce(i + 1, if (column.exists(row)) acc |+| column.value(row) else acc)
    } else acc

    state.add(keys(start), reduce(start, Monoid[V].id))
    state
  }
}
