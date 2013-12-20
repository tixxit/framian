package pellucid.pframe
package reduce

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

private[reduce] final class Quantile[A: Field: Order: ClassTag](probabilities: Seq[Double]) extends Reducer[A, Seq[(Double, A)]] {

  def quantiles(s: Seq[A]) = {
    def interpolate(x: Double, pos: (Double, A), pos1: (Double, A)) =
      pos._2 + (pos1._2 - pos._2) * (x - pos._1) / (pos1._1 - pos._1)

    val points = s.toArray.sorted
    val segments = points.length
    val segmentSize = (1d / segments)
    val segmentMedian = segmentSize / 2d
    val pointsWithQuantile = points.zipWithIndex.map { case (y, i) =>
      val start = i * segmentSize
      val position = start + segmentMedian
      (position, y)
    }

    val (_, probabilityRanges) =
      probabilities.sorted.foldLeft((pointsWithQuantile, List[(Double, A)]())) {
        case ((remainingPoints, accum), newProbability) =>
          val rangeIndex = remainingPoints.indexWhere(_._1 > newProbability)
          val rangeStart =
            if (rangeIndex == -1) 0
            else if (rangeIndex == (remainingPoints.length - 1)) (rangeIndex - 1)
            else rangeIndex
          val rangeEnd = rangeStart + 1
          val (_, remainder) = remainingPoints.splitAt(rangeStart)

          (remainder,
           (newProbability,
            interpolate(newProbability, remainingPoints(rangeStart), remainingPoints(rangeEnd))) :: accum)
      }

    probabilityRanges
  }

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Seq[(Double, A)] = {
    val existingColumnValues = indices.slice(start, end) flatMap { i => column(i) }
    quantiles(existingColumnValues)
  }
}
