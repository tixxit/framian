package framian.column

import java.lang.Integer.highestOneBit
import java.lang.Long.bitCount
import java.util.Arrays

import scala.annotation.tailrec
import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuilder

import spire.implicits._

class MaskBuilder {
  var len = 0
  var size = 0
  var bits = new Array[Long](8)

  private def resize(newLen: Int): Unit = {
    bits = Arrays.copyOf(bits, highestOneBit(newLen) * 2)
    len = newLen
  }

  def +=(n: Int): this.type = {
    val i = n >>> 6
    if (i >= bits.length)
      resize(i + 1)
    if (i >= len)
      len = i + 1
    val word = bits(i)
    val bit = 1L << (n & 0x3F)
    if ((word & bit) == 0) {
      bits(i) = word | bit
      size += 1
    }
    this
  }

  def result(): Mask = {
    val bits0 = Arrays.copyOf(bits, len)
    new Mask(bits0, size)
  }
}

object Mask {
  def newBuilder: MaskBuilder = new MaskBuilder

  final val empty = new Mask(new Array[Long](0), 0)

  def apply(elems: Int*): Mask = {
    val bldr = new MaskBuilder
    elems.foreach(bldr += _)
    bldr.result()
  }

  def range(from: Int, until: Int): Mask =
    Mask(from.until(until): _*)

  final def fromBits(bits: Array[Long]): Mask = {
    var i = 0
    var size = 0
    while (i < bits.length) {
      size += bitCount(bits(i))
      i += 1
    }
    new Mask(bits, size)
  }
}

final class Mask(private val bits: Array[Long], val size: Int) {
  def foreach[U](f: Int => U): Unit = {
    var i = 0
    while (i < bits.length) {
      val word = bits(i)
      val hi = i << 6
      var lo = 0
      while (lo < 64) {
        if ((word & (1L << lo)) != 0)
          f(hi | lo)
        lo += 1
      }
      i += 1
    }
  }

  def |(that: Mask): Mask = {
    val size = math.max(bits.length, that.bits.length)
    val bits0 = Arrays.copyOf(that.bits, size)
    var i = 0
    while (i < bits.length) {
      bits0(i) |= bits(i)
      i += 1
    }
    Mask.fromBits(bits0)
  }

  def &(that: Mask): Mask = {
    val size = math.min(bits.length, that.bits.length)
    val bits0 = Arrays.copyOf(that.bits, size)
    var i = 0
    while (i < bits0.length) {
      bits0(i) &= bits(i)
      i += 1
    }
    Mask.fromBits(bits0)
  }

  final def ++(that: Mask): Mask = this | that

  def --(that: Mask): Mask = {
    val bldr = new MaskBuilder
    foreach { i => if (!that(i)) bldr += i }
    bldr.result()
  }

  def +(n: Int): Mask = {
    val hi = n >>> 6
    val bit = 1L << (n & 0x3F)

    if (hi < bits.length && (bits(hi) & bit) == 0) {
      this
    } else {
      val len = math.max(bits.length, n)
      val bits0 = Arrays.copyOf(bits, len)
      bits0(hi) |= bit
      new Mask(bits0, size + 1)
    }
  }

  def filter(f: Int => Boolean): Mask = {
    val bldr = new MaskBuilder
    foreach { i => if (f(i)) bldr += i }
    bldr.result()
  }

  def apply(n: Int): Boolean = {
    val hi = n >>> 6
    if (hi < bits.length)
      (bits(hi) & (1L << (n & 0x3F))) != 0L
    else
      false
  }

  def toSet: Set[Int] = {
    val bldr = Set.newBuilder[Int]
    foreach(bldr += _)
    bldr.result()
  }

  override def toString: String =
    toSet.mkString("Mask(", ", ", ")")
}

// A failed experiment.

//private[column] final class Hash(p: Int, a: Long, b: Long) {
//  def apply(n: Int): Int = ((a * n + b) % p).toInt
//}
//
//private[column] object Hash {
//  val h0 = new Hash(2008589393, 251867421, 1849271569)
//  val h1 = new Hash(1656979913, 492859796, 636215185)
//}
//
//class MaskBuilder {
//  val bldr: ArrayBuilder[Int] = ArrayBuilder.make()
//
//  def +=(i: Int): this.type = {
//    bldr += i
//    this
//  }
//
//  def result(): Mask = Mask.fromArray(bldr.result())
//}
//
//object Mask {
//  def newBuilder: MaskBuilder = new MaskBuilder
//
//  final val empty = fromArray(new Array[Int](0))
//
//  def apply(elems: Int*): Mask = {
//    val bldr = newBuilder
//    elems.foreach(bldr += _)
//    bldr.result()
//  }
//
//  def range(from: Int, until: Int): Mask =
//    Mask.fromArray(Array.range(from, until))
//
//  def fromArray(values: Array[Int]): Mask = {
//    val hash0: Hash = Hash.h0
//    val hash1: Hash = Hash.h1
//
//    val stride = 2
//    val bitsPerEntry = 32 * (stride - 1)
//    val entries = countEntries(values, bitsPerEntry)
//    val tableSize = math.max(1, highestOneBit(entries * 3 / 2) * 2)
//    val table = new Array[Int](tableSize * stride)
//    val mask = tableSize - 1
//
//    def add(n: Int): Boolean = {
//      val key = ((n & 0xFFFFFFFFL) / bitsPerEntry).toInt
//      val h0 = hash0(key)
//      val h1 = hash1(key)
//
//      @tailrec
//      def loop(i: Int): Boolean = {
//        val h2 = if (i > 4) 1 else h1 // TODO: Get rid of branch?
//        val h = ((h0 + i * h2) & mask) * stride
//        val entry = table(h)
//        val status = entry & 0xF0000000
//        val low = n % bitsPerEntry
//        val word = h + low / 32 + 1
//        val bit = 1 << (low % 32)
//        if (status == 0) {
//          table(h) = key | (1 << 28)
//          table(word) = bit
//          true
//        } else if ((entry & 0x0FFFFFFF) == key) {
//          val added = (table(word) & bit) == 0
//          table(word) |= bit
//          added
//        } else {
//          loop(i + 1)
//        }
//      }
//
//      loop(0)
//    }
//
//    var i = 0
//    var size = 0
//    while (i < values.length) {
//      if (add(values(i)))
//        size += 1
//      i += 1
//    }
//
//    new Mask(table, stride, mask, size, hash0, hash1)
//  }
//
//  private def countEntries(xs: Array[Int], bitsPerEntry: Int): Int =
//    if (xs.length == 0) 0 else {
//      val ys = Arrays.copyOf(xs, xs.length)
//      ys.qsort
//
//      @tailrec
//      def count(i: Int, j: Int): Int = if (j < ys.length) {
//        val k = ys(j) / bitsPerEntry
//        if (ys(i) != k) {
//          ys(i + 1) = k
//          count(i + 1, j + 1)
//        } else {
//          count(i, j + 1)
//        }
//      } else {
//        i + 1
//      }
//
//      ys(0) /= bitsPerEntry
//      count(0, 1)
//    }
//}
//
//// A BitSet implementation that aims to have good cache locality when accessed
//// in sorted order.
//final class Mask(table: Array[Int], stride: Int, mask: Int, val size: Int, hash0: Hash, hash1: Hash) {
//  private val bitsPerEntry = 32 * (stride - 1)
//
//  def toSet: Set[Int] = {
//    val bldr = Set.newBuilder[Int]
//    foreach(bldr += _)
//    bldr.result()
//  }
//
//  def foreach[U](f: Int => U): Unit = {
//    var i = 0
//    while (i < table.size) {
//      if ((table(i) & 0xF0000000) != 0) {
//        val hi = (table(i) & 0x0FFFFFFF) * bitsPerEntry
//        var lo = 0
//        while (lo < bitsPerEntry) {
//          val word = i + lo / 32 + 1
//          val bit = 1 << (lo % 32)
//          if ((table(word) & bit) != 0) {
//            f(hi | lo)
//          }
//          lo += 1
//        }
//      }
//      i += stride
//    }
//  }
//
//  def |(that: Mask): Mask = {
//    val bldr = new MaskBuilder
//    foreach(bldr += _)
//    that.foreach { i =>
//      if (!apply(i)) bldr += i
//    }
//    bldr.result()
//  }
//
//  def &(that: Mask): Mask = {
//    if (this.size < that.size) {
//      val bldr = new MaskBuilder
//      foreach { i =>
//        if (that(i))
//          bldr += i
//      }
//      bldr.result()
//    } else {
//      that & this
//    }
//  }
//
//  def --(that: Mask): Mask = {
//    val bldr = new MaskBuilder
//    foreach { i => if (!that(i)) bldr += i }
//    bldr.result()
//  }
//
//  def ++(that: Mask): Mask = {
//    val bldr = new MaskBuilder
//    this.foreach(bldr += _)
//    that.foreach(bldr += _)
//    bldr.result()
//  }
//
//  def filter(p: Int => Boolean): Mask = {
//    val bldr = new MaskBuilder
//    foreach { i => if (p(i)) bldr += i }
//    bldr.result()
//  }
//
//  def map(f: Int => Int): Mask = {
//    val bldr = new MaskBuilder
//    foreach { i => bldr += f(i) }
//    bldr.result()
//  }
//
//  final def apply(n: Int): Boolean = {
//    val key = ((n & 0xFFFFFFFFL) / bitsPerEntry).toInt
//    val h0 = hash0(key)
//    val h1 = hash1(key)
//
//    @tailrec
//    def loop(i: Int): Boolean = {
//      val h2 = if (i > 4) 1 else h1 // TODO: Get rid of branch?
//      val h = ((h0 + i * h2) & mask) * stride
//      val entry = table(h)
//      val status = entry & 0xF0000000
//      val low = n % bitsPerEntry
//      val word = h + low / 32 + 1
//      val bit = 1 << (low % 32)
//      if (status == 0) {
//        false
//      } else if ((entry & 0x0FFFFFFF) == key) {
//        (table(word) & bit) != 0
//      } else {
//        loop(i + 1)
//      }
//    }
//
//    loop(0)
//  }
//
//  override def toString: String =
//    toSet.mkString("Mask(", ", ", ")")
//}
