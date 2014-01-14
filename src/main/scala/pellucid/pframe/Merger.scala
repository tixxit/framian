package pellucid.pframe

import scala.reflect.ClassTag
import scala.collection.mutable.{ ArrayBuilder, Builder }

import spire.syntax.cfor._

sealed abstract class Merge(val outer: Boolean)

object Merge {
  case object Inner extends Merge(false) // intersection
  case object Outer extends Merge(true) // union
}

/**
 * This implements a [[Cogrouper]] that is suitable for generating the indices
 * necessary for merges and appends on [[Series]] and [[Frame]].
 */
final case class Merger[K: ClassTag](merge: Merge) extends Index.GenericJoin[K] {
  import Index.GenericJoin.Skip

  def cogroup(state: State)(
    lKeys: Array[K], lIdx: Array[Int], lStart: Int, lEnd: Int,
    rKeys: Array[K], rIdx: Array[Int], rStart: Int, rEnd: Int): State = {

    if (lEnd > lStart && rEnd > rStart) {
      val key = lKeys(lStart)

      var rPosition = rStart
      var lPosition = lStart

      if (merge.outer) while (lPosition < lEnd || rPosition < rEnd) {
        val li = if (lPosition >= lEnd) Skip else lIdx(lPosition)
        val ri = if (rPosition >= rEnd) Skip else rIdx(rPosition)
        lPosition += 1
        rPosition += 1
        state.add(key, li, ri)
      } else while (lPosition < lEnd && rPosition < rEnd) {
        state.add(key, lIdx(lPosition), rIdx(rPosition))
        lPosition += 1
        rPosition += 1
      }
    } else if (merge.outer) {
      if (lEnd > lStart) {
        val key = lKeys(lStart)
        cfor(lStart)(_ < lEnd, _ + 1) { i =>
          state.add(key, lIdx(i), Skip)
        }
      } else if (rEnd > rStart) {
        val key = rKeys(rStart)
        cfor(rStart)(_ < rEnd, _ + 1) { i =>
          state.add(key, Skip, rIdx(i))
        }
      }
    }

    state
  }
}
