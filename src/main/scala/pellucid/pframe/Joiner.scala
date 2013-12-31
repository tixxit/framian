package pellucid.pframe

import scala.reflect.ClassTag
import scala.collection.mutable.{ ArrayBuilder, Builder }

import spire.syntax.cfor._

sealed abstract class Join(val leftOuter: Boolean, val rightOuter: Boolean)

object Join {
  case object Inner extends Join(false, false)
  case object Left extends Join(true, false)
  case object Right extends Join(false, true)
  case object Outer extends Join(true, true)
}

/**
 * This implements a [[Cogrouper]] that is suitable for generating the indices
 * necessary for joins on [[Series]] and [[Frame]].
 */
final case class Joiner[K: ClassTag](join: Join) extends Index.Cogrouper[K] {
  import Joiner._

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
      cfor(lStart)(_ < lEnd, _ + 1) { i =>
        val li = lIdx(i)
        cfor(rStart)(_ < rEnd, _ + 1) { j =>
          state.add(key, li, rIdx(j))
        }
      }
    } else if (lEnd > lStart && join.leftOuter) {
      val key = lKeys(lStart)
      cfor(lStart)(_ < lEnd, _ + 1) { i =>
        state.add(key, lIdx(i), Skip)
      }
    } else if (rEnd > rStart && join.rightOuter) {
      val key = rKeys(rStart)
      cfor(rStart)(_ < rEnd, _ + 1) { i =>
        state.add(key, Skip, rIdx(i))
      }
    }

    val tempResults = state.result()
    println(tempResults._1.mkString(" "))
    println(tempResults._2.mkString(" "))
    println(tempResults._3.mkString(" "))
    state
  }
}

object Joiner {
  final val Skip = Int.MinValue
}
