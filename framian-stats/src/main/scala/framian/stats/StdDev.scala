package framian
package stats

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire.algebra._
import spire.syntax.field._
import spire.syntax.nroot._

import framian.reduce.Reducer

final case class StdDev[A](count: Int, mean: A, stdDev: A) {
  def variance(implicit A: Semiring[A]): A = stdDev * stdDev
}

object StdDev {
  def fromArray[A: Field: NRoot](data: Array[A]): Option[StdDev[A]] = {
    val bldr = builder[A]
    data foreach { bldr += _ }
    bldr.result()
  }

  def builder[A: Field]: Builder[A] =
    new Builder(0, Field[A].zero, Field[A].zero)

  final class Builder[A](var count: Int, var mean: A, var sumSq: A) {
    def +=(a: A)(implicit A: Field[A]): Unit = {
      val delta = a - mean
      count += 1
      mean += delta / count
      sumSq += delta * (a - mean)
    }

    def result()(implicit A0: Field[A], A1: NRoot[A]): Option[StdDev[A]] =
      if (count == 0) None
      else if (count == 1) Some(StdDev(1, mean, A0.zero))
      else Some(StdDev(count, mean, (sumSq / (count - 1)).sqrt))
  }

  def reducer[A: Field: NRoot: ClassTag]: Reducer[A, StdDev[A]] =
    Reducer[A, StdDev[A]](data => Cell.fromOption(StdDev.fromArray(data)))
}
