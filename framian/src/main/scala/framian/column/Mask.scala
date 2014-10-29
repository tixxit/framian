/*  _____                    _
 * |  ___| __ __ _ _ __ ___ (_) __ _ _ __
 * | |_ | '__/ _` | '_ ` _ \| |/ _` | '_ \
 * |  _|| | | (_| | | | | | | | (_| | | | |
 * |_|  |_|  \__,_|_| |_| |_|_|\__,_|_| |_|
 *
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package framian
package column

import java.lang.Integer.highestOneBit
import java.lang.Long.bitCount
import java.util.Arrays

import scala.annotation.tailrec
import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuilder

import spire.implicits._

/**
 * A `Mask` provides a dense bitset implementation. This replaces uses of
 * `BitSet`. The major difference is that we don't box the `Int`s.
 *
 * An explanation of some of the arithmetic you'll see here:
 *
 * We store the bits in array of words. Each word contains 64 bits and the
 * words are in order. So, the word containing bit `n` is `n &gt;&gt;&gt; 6` -
 * we simply drop the lower 6 bits (divide by 64). If `n` is set, then the bit
 * `n &amp; 0x3FL`, in word `bits(n &gt;&gt;&gt; 6)` will be `true`. We can
 * check this by masking the word with `1L &lt;&lt; (n &amp; 0x3FL)` and
 * checking if the result is non-zero.
 *
 * An invariant of the underlying `bits` array is that the highest order word
 * (ie. `bits(bits.length - 1)`) is always non-zero (except if `bits` has 0
 * length). This sometimes means we must *trim* the array for some operations
 * that could possibly zero out the highest order word (eg. intersection and
 * subtraction.
 */
final class Mask(private val bits: Array[Long], val size: Int) extends (Int => Boolean) {
  import Mask.trim

  def max: Option[Int] =
    if (bits.length == 0) {
      None
    } else {
      // An invariant of all Masks is that the highest order word always has
      // at least 1 bit set. So, we can just loop through all 64 bits in the
      // highest word (highest to lowest) and return the first set one.
      val hi = bits.length - 1
      val word = bits(hi)
      var i = 64
      while (i > 0) {
        i -= 1
        if ((word & (1L << i)) != 0)
          return Some((hi << 6) | i)
      }
      None
    }

  def min: Option[Int] =
    if (bits.length == 0) {
      None
    } else {
      // We can cheat here and simply use `foreach` + `return`.
      foreach(i => return Some(i))
      None
    }

  def isEmpty: Boolean = size == 0

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
    // We can simply | all the words together. The new bits array will be as
    // long as the largest of this.bits and that.bits.
    val size = math.max(bits.length, that.bits.length)
    // copyOf will zero out the top bits, which is exactly what we want.
    val bits0 = Arrays.copyOf(that.bits, size)
    var i = 0
    while (i < bits.length) {
      bits0(i) |= bits(i)
      i += 1
    }
    Mask.fromBits(bits0)
  }

  def &(that: Mask): Mask = {
    // We can simply & all the words together. The new bits array will be as
    // long as the shortest of this.bits and that.bits.
    val size = math.min(bits.length, that.bits.length)
    val bits0 = Arrays.copyOf(that.bits, size)
    var i = 0
    while (i < bits0.length) {
      val word = bits0(i) & bits(i)
      bits0(i) = word
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
    val hi = n >>> 6           // The offset of the word this bit is in.
    val bit = 1L << (n & 0x3F) // The bit position in the word n is in.

    if (hi < bits.length && (bits(hi) & bit) != 0) {
      // The bit is already set, so we're done!
      this
    } else {
      val len = math.max(bits.length, hi + 1)
      val bits0 = Arrays.copyOf(bits, len)
      bits0(hi) |= bit
      new Mask(bits0, size + 1)
    }
  }

  def -(n: Int): Mask = {
    val hi = n >>> 6
    val bit = 1L << (n & 0x3F)

    if (hi < bits.length && (bits(hi) & bit) != 0) {
      val bits0 = Arrays.copyOf(bits, bits.length)
      val word = bits0(hi) ^ bit // This will flip the bit off without changing the others.
      bits0(hi) = word
      new Mask(trim(bits0), size - 1)
    } else {
      // The bit isn't set, so we're done!
      this
    }
  }

  def filter(f: Int => Boolean): Mask = {
    val bldr = new MaskBuilder
    foreach { i => if (f(i)) bldr += i }
    bldr.result()
  }

  def map(f: Int => Int): Mask = {
    val bldr = new MaskBuilder
    foreach { i => bldr += f(i) }
    bldr.result()
  }

  def apply(n: Int): Boolean = {
    val hi = n >>> 6 // The offset of the word containing bit n.
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

  def toBitSet: BitSet = {
    val bldr = BitSet.newBuilder
    foreach(bldr += _)
    bldr.result()
  }

  override def toString: String =
    toSet.mkString("Mask(", ", ", ")")

  override def equals(that: Any): Boolean = that match {
    case (that: Mask) if this.size == that.size =>
      var i = 0
      while (i < bits.length) {
        val w0 = bits(i)
        val w1 = that.bits(i)
        if ((w0 ^ w1) != 0)
          return false
        i += 1
      }
      true

    case _ => false
  }

  override def hashCode: Int =
    bits.foldLeft(1914323553)(_ ^ _.hashCode)
}

object Mask {
  def newBuilder: MaskBuilder = new MaskBuilder

  /** An empty mask where all bits are unset. */
  final val empty = new Mask(new Array[Long](0), 0)

  /**
   * Returns a [[Mask]] where only the bits in `elems` are set to true and all
   * others are false.
   */
  def apply(elems: Int*): Mask = {
    val bldr = new MaskBuilder
    elems.foreach(bldr += _)
    bldr.result()
  }

  /**
   * Returns a [[Mask]] with all bits from `from` to `until` (exclusive) set
   * and all others unset.
   */
  def range(from: Int, until: Int): Mask = {
    val bldr = new MaskBuilder
    var i = from
    while (i < until) {
      bldr += i
      i += 1
    }
    bldr.result()
  }

  /**
   * Create a Mask from an array of words representing the actual bit mask
   * itself. Please see [[Mask]] for a description of what `bits` should look
   * like.
   */
  final def fromBits(bits: Array[Long]): Mask = {
    var i = 0
    var size = 0
    while (i < bits.length) {
      size += bitCount(bits(i))
      i += 1
    }
    new Mask(trim(bits), size)
  }

  // We need to ensure the highest order word is not 0, so we work backwards
  // and find the first, non-zero word, then trim the array so it becomes the
  // highest order word.
  private def trim(bits: Array[Long]): Array[Long] = {
    var i = bits.length
    while (i > 0 && bits(i - 1) == 0) {
      i -= 1
    }
    if (i == bits.length) bits else Arrays.copyOf(bits, i)
  }
}

final class MaskBuilder {
  // bits.length may be larger than we need it, so `len` is the actual minimal
  // length required to store the bitset, with the highest order, non-zero word
  // at bits(len - 1).
  var len = 0

  // The total number of 1 bits in the bitset.
  var size = 0

  // The packed bitset.
  var bits = new Array[Long](8)

  /**
   * Occasionally we have to enlarge the array if we haven't allocated enough
   * storage. We attempt to ~double the size of the current array.
   */
  private def resize(newLen: Int): Unit = {
    // Note: we won't ever require an array larger than 0x03FFFFFF, so we don't
    // need to worry about the length overflowing below.
    bits = Arrays.copyOf(bits, highestOneBit(newLen) * 2)
    len = newLen
  }

  def +=(n: Int): this.type = {
    val i = n >>> 6 // The offset of the word containing the bit n.
    if (i >= bits.length)
      resize(i + 1)
    if (i >= len)
      len = i + 1
    val word = bits(i)
    val bit = 1L << (n & 0x3F)
    if ((word & bit) == 0) {
      // The bit isn't already set, so we add it and increase the size.
      bits(i) = word | bit
      size += 1
    }
    this
  }

  def result(): Mask = {
    val bits0 = Arrays.copyOf(bits, len)
    new Mask(bits0, size)
  }

  def clear(): Unit = {
    len = 0
    size = 0
    bits = new Array[Long](8)
  }
}
