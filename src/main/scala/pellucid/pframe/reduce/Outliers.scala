package pellucid.pframe
package reduce

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire._
import spire.compat._
import spire.algebra.{Field, Order}
import spire.syntax.field._
import spire.syntax.order._

final class Outliers[A: Field: Order: ClassTag] extends SimpleReducer[A, (Option[A], Option[A])] {
  val quantiler = new Quantile[A](Seq(.25, .75))

  def reduce(data: Array[A]): Value[(Option[A], Option[A])] = {
    val (_, q1) :: (_, q3) :: Nil = quantiler.quantiles(data)

    val iqr = q3 - q1
    val lowerFence = q1 - (1.5 * iqr)
    val upperFence = q3 + (1.5 * iqr)

    val lowerOutliers = data.filter(_ <= lowerFence)
    val upperOutliers = data.filter(_ >= upperFence)

    Value((if (lowerOutliers.length > 0) Some(lowerFence) else None,
           if (upperOutliers.length > 0) Some(upperFence) else None))
  }
}
