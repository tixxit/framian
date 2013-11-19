package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{ TypeTag, typeTag }
import scala.annotation.tailrec

import spire.algebra.Order
import spire.math.Searching
import shapeless._

import spire.syntax.cfor._

trait Index[K] {
  def size: Int
  def apply(k: K): Int
  def get(k: K): Option[Int] = {
    val i = apply(k)
    if (i >= 0) Some(i) else None
  }

  def foreach[U](f: (K, Int) => U): Unit

  def keys: Seq[K]
  def indices: Array[Int]
}

object Index {
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

  def ordered[K: Order](keys: Array[K]): Index[K] =
    new OrderedIndex(keys)

  def unordered[K: Order: ClassTag](keys: Array[K]): Index[K] = {
    import spire.syntax.std.array._

    def flip(xs: Array[Int]): Array[Int] = {
      val ys = new Array[Int](xs.size)
      cfor(0)(_ < xs.size, _ + 1) { i =>
        ys(xs(i)) = i
      }
      ys
    }

    val indices = flip(Array.range(0, keys.length).qsortedBy(keys(_)))
    new UnorderedIndex(keys.qsorted, indices)
  }
}

final class UnorderedIndex[K: Order](keys0: Array[K], indices0: Array[Int]) extends Index[K] {
  def order: Seq[K] = indices0 map (keys0(_))
  def size: Int = keys0.size
  def apply(k: K): Int = {
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

  def foreach[U](f: (K, Int) => U): Unit = {
    cfor(0)(_ < indices0.length, _ + 1) { i =>
      val idx = indices0(i)
      val key = keys0(idx)
      f(key, idx)
    }
  }

  def keys = indices0 map (keys0(_))
  def indices = indices0.clone()
}

final class OrderedIndex[K: Order](keys0: Array[K]) extends Index[K] {
  def keys = keys0
  def indices = Array.range(0, keys0.length)
  def size: Int = keys0.size
  def apply(k: K): Int = Searching.search(keys0, k)

  def foreach[U](f: (K, Int) => U): Unit = {
    cfor(0)(_ < keys0.length, _ + 1) { i =>
      f(keys0(i), i)
    }
  }
}
