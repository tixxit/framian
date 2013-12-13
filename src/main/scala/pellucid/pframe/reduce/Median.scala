package pellucid.pframe
package reduce

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

private[reduce] final class Median[A: Field: Order: ClassTag] extends Reducer[A, Option[A]] {

  implicit def chooseRandomPivot(arr: Array[Double]): Double = arr(scala.util.Random.nextInt(arr.size))

  @tailrec def findKMedian(arr: Array[Double], kOrValue: Either[Int, Double], k2OrValue: Either[Int, Double])
                          (implicit choosePivot: Array[Double] => Double): (Double, Double) = {
    val a = choosePivot(arr)
    (kOrValue, k2OrValue) match {
      case (Right(v1), Right(v2)) =>
        (v1, v2)
      case (Left(k), Left(k2)) =>
        val (s, b) = arr partition { x => a > x }
        if (s.size == k) findKMedian(arr, Right(a), Left(k2))
        else if (s.size == k2) findKMedian(arr, Left(k), Right(a))
        else if (s.isEmpty) {
          val (s, b) = arr partition (a ==)
          if (s.size > k && s.size > k2) (a, a)
          else if (s.size > k) findKMedian(arr, Right(a), Left(k2))
          else if (s.size > k2) findKMedian(arr, Left(k), Right(a))
          else findKMedian(b, Left(k - s.size), Left(k2 - s.size))
        } else if (s.size < k && s.size < k2) findKMedian(b, Left(k - s.size), Left(k2 - s.size))
          else if (s.size < k) findKMedian(b, Left(k - s.size), Left(k2))
          else if (s.size < k2) findKMedian(b, Left(k), Left(k2 - s.size))
        else findKMedian(s, Left(k), Left(k2))

      case (Left(k), Right(v)) =>
        val (s, b) = arr partition { x => a > x }
        if (s.size == k)  (a, v)
        else if (s.isEmpty) {
          val (s, b) = arr partition (a ==)
          if (s.size > k) (a, v)
          else findKMedian(b, Left(k - s.size), k2OrValue)
        } else if (s.size < k) findKMedian(b, Left(k - s.size), k2OrValue)
          else findKMedian(s, Left(k), k2OrValue)

      case (Right(v), Left(k)) =>
        val (s, b) = arr partition { x => a > x }
        if (s.size == k)  (v, a)
        else if (s.isEmpty) {
          val (s, b) = arr partition (a ==)
          if (s.size > k) (v, a)
          else findKMedian(b, kOrValue, Left(k - s.size))
        } else if (s.size < k) findKMedian(b, kOrValue, Left(k - s.size))
          else findKMedian(s, kOrValue, Left(k))
    }
  }

  def findMedian(arr: Array[Double])(implicit choosePivot: Array[Double] => Double) = {
    if (arr.size % 2 == 0) {
      val (left, right) = findKMedian(arr, Left(arr.size / 2), Left((arr.size / 2) - 1))
      (left / 2) + (right / 2)
    } else findKMedian(arr, Left((arr.size - 1) / 2), Left((arr.size - 1) / 2))._1
  }

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Option[A] = {
    val existingColumnValues = indices.slice(start, end) flatMap { i => column(i) }

    if (existingColumnValues.isEmpty) None
    else Some(findMedian(existingColumnValues))
  }
}
