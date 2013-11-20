package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{ TypeTag, typeTag }
import scala.annotation.tailrec
import scala.collection.{ IterableLike, Iterable }
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ ArrayBuilder, Builder }

import spire.algebra.Order
import spire.math.Searching
import shapeless._

import spire.syntax.cfor._

trait Index[K] extends Iterable[(K, Int)] with IterableLike[(K, Int), Index[K]] {
  implicit def classTag: ClassTag[K]
  implicit def order: Order[K]

  def empty: Index[K] = Index.empty[K]

  def size: Int

  def iterator: Iterator[(K, Int)]

  def search(k: K): Int

  override def seq: Index[K] = this

  override protected def newBuilder: Builder[(K, Int), Index[K]] =
    new Index.IndexBuilder

  def get(k: K): Option[Int] = {
    val i = search(k)
    if (i >= 0) Some(i) else None
  }

  def keys: Seq[K]
  def indices: Array[Int]
}

object Index {
  implicit def cbf[K: Order: ClassTag]: CanBuildFrom[Index[_], (K, Int), Index[K]] =
    new CanBuildFrom[Index[_], (K, Int), Index[K]] {
      def apply(): Builder[(K, Int), Index[K]] = new IndexBuilder[K]
      def apply(from: Index[_]): Builder[(K, Int), Index[K]] = apply()
    }

  def empty[K: Order: ClassTag]: Index[K] = new OrderedIndex[K](new Array[K](0), new Array[Int](0))

  def apply[K: Order: ClassTag](keys: K*): Index[K] =
    unordered(keys.toArray)

  def apply[K: Order: ClassTag](keys: Array[K]): Index[K] = {
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
      ordered(keys)
    } else {
      unordered(keys)
    }
  }

  def ordered[K: Order: ClassTag](pairs: Seq[(K, Int)]): Index[K] = {
    val (keys, indices) = pairs.unzip
    new OrderedIndex(keys.toArray, indices.toArray)
  }

  def ordered[K: Order: ClassTag](keys: Array[K]): Index[K] =
    new OrderedIndex(keys, Array.range(0, keys.length))

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

    // TODO: We can probably do better here. Especially once qsearchBy lands in Spire.

    val order0 = Array.range(0, keys.length).qsortedBy(keys(_))
    val indices0 = shuffle(indices, order0)
    val keys0 = shuffle(keys, order0)
    new UnorderedIndex(keys0, flip(order0), indices0)
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
}

abstract class BaseIndex[K](implicit
    val order: Order[K], val classTag: ClassTag[K]) extends Index[K]

final class UnorderedIndex[K: Order: ClassTag](
      keys0: Array[K], order0: Array[Int], indices0: Array[Int])
    extends BaseIndex[K] {

  def keys = order0 map (keys0(_))
  def indices = order0.clone()

  override def size: Int = order0.size
  def search(k: K): Int = {
    val i = Searching.search(keys0, k)
    if (i < 0) {
      val j = -i - 1
      if (j < indices0.length) {
        -indices0(-i - 1) - 1
      } else {
        -j - 1
      }
    } else {
      indices0(i)
    }
  }

  def iterator: Iterator[(K, Int)] = order0.iterator map { i =>
    (keys0(i), i)
  }

  override def foreach[U](f: ((K, Int)) => U): Unit = {
    cfor(0)(_ < order0.length, _ + 1) { i =>
      val ord = order0(i)
      f((keys0(ord), indices0(ord)))
    }
  }
}

final class OrderedIndex[K: Order: ClassTag](keys0: Array[K], indices0: Array[Int]) extends BaseIndex[K] {
  def keys = keys0
  def indices = indices0.clone()
  override def size: Int = keys0.size
  def search(k: K): Int = Searching.search(keys0, k)
  def iterator: Iterator[(K, Int)] = Iterator.tabulate(keys0.length) { i =>
    (keys0(i), indices0(i))
  }

  override def foreach[U](f: ((K, Int)) => U): Unit = {
    cfor(0)(_ < keys0.length, _ + 1) { i =>
      f((keys0(i), indices0(i)))
    }
  }
}
