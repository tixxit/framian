package pellucid.pframe

import scala.reflect.ClassTag
import scala.collection.mutable.{ ArrayBuilder, Builder }

import spire.syntax.cfor._

//sealed abstract class Join(val leftOuter: Boolean, val rightOuter: Boolean)
sealed abstract class Merge(val outer: Boolean)

object Merge {
  case object Inner extends Merge(false) // intersection
  case object Outer extends Merge(true) // union
}

/**
 * This implements a [[Cogrouper]] that is suitable for generating the indices
 * necessary for merges and appends on [[Series]] and [[Frame]].
 */
final case class Merger[K: ClassTag](merge: Merge) extends Index.Cogrouper[K] {

  import Merger._

  // We cheat here and use a mutable state because an immutable one would just
  // be too slow.
  final class State {
    val keys: ArrayBuilder[K] = ArrayBuilder.make[K]
    val lIndices: ArrayBuilder[Int] = ArrayBuilder.make[Int]
    val rIndices: ArrayBuilder[Int] = ArrayBuilder.make[Int]

    def add(k: K, l: Int, r: Int) { keys += k; lIndices += l; rIndices += r }

    def result(): (Array[K], Array[Int], Array[Int]) =
      (keys.result(), lIndices.result(), rIndices.result())
  }

  def init = new State

  def cogroup(state: State)(
    lKeys: Array[K], lIdx: Array[Int], lStart: Int, lEnd: Int,
    rKeys: Array[K], rIdx: Array[Int], rStart: Int, rEnd: Int): State = {

    println("-----")
    println("lKeys: "+ lKeys.mkString(" "))
    println("rKeys: "+ rKeys.mkString(" "))
    println("lIdx: "+ lIdx.mkString(" "))
    println("rIdx: "+ rIdx.mkString(" "))
    println("lStart: "+ lStart)
    println("rStart: "+ rStart)
    println("lEnd: "+ lEnd)
    println("rEnd: "+ rEnd)
    println("-----")


    if (lEnd > lStart && rEnd > rStart) {
      val key = lKeys(lStart)
      if (merge.outer) {
        val lSize = lIdx.length
        val rSize = rIdx.length

        var rPosition = rStart
        var lPosition = lStart
        while (lPosition < lEnd || rPosition < rEnd) {
          val li = if (lPosition >= lEnd) Skip else lIdx(lPosition)
          val ri = if (rPosition >= rEnd) Skip else rIdx(rPosition)
          if (li == ri || li == Skip || ri == Skip) {
            lPosition += 1
            rPosition += 1
            state.add(key, li, ri)
          } else if (li < ri) {
            lPosition += 1
            state.add(key, li, Skip)
          } else if (ri < li) {
            rPosition += 1
            state.add(key, Skip, ri)
          }
        }
      } else {
        cfor(lStart)(_ < lEnd, _ + 1) { i =>
          val li = lIdx(i)
          cfor(rStart)(_ < rEnd, _ + 1) { j =>
            val ri = rIdx(j)
            if (li == ri) state.add(key, li, ri)
          }
        }
      }
    } else if (lEnd > lStart) {
      val key = lKeys(lStart)
      if (merge.outer) {
        cfor(lStart)(_ < lEnd, _ + 1) { i =>
          state.add(key, lIdx(i), Skip)
        }
      }
    } else if (rEnd > rStart) {
      val key = rKeys(rStart)
      if (merge.outer) {
        cfor(rStart)(_ < rEnd, _ + 1) { i =>
          state.add(key, Skip, rIdx(i))
        }
      }
    }

    val tempResults = state.result()
    println(tempResults._1.mkString(" "))
    println(tempResults._2.mkString(" "))
    println(tempResults._3.mkString(" "))
    state
  }
}

object Merger {
  final val Skip = Int.MinValue
}
