package pellucid.pframe
package reduce

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

private[reduce] final class Outliers[A: Field: Order: ClassTag] extends Reducer[A, (Option[A], Option[A])] {

  val quantiler = new Quantile[A](Seq(.25, .75))

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): (Option[A], Option[A]) = {
    val existingColumnValues = indices.slice(start, end) flatMap { i => column(i) }
    val Seq((_, q1), (_, q3)) = quantiler.reduce(column, indices, start, end)

    val iqr = q3 - q1
    val lowerFence = q1 - (1.5 * iqr)
    val upperFence = q3 + (1.5 * iqr)

    val lowerOutliers = existingColumnValues.filter(_ <= lowerFence)
    val upperOutliers = existingColumnValues.filter(_ >= upperFence)

    (if (lowerOutliers.length > 0) Some(lowerFence) else None,
     if (upperOutliers.length > 0) Some(upperFence) else None)
  }
}
