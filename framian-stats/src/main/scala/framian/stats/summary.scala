package framian
package stats

import scala.reflect.ClassTag

import spire.algebra._
import spire.math.Number
import spire.std.string._
import spire.syntax.std.array._
import spire.syntax.field._

import framian.reduce.Reducer

case class Summary[A](
  mean: A,
  median: A,
  min: A,
  max: A)

object Summary {
  val Mean = "Mean"
  val Median = "Median"
  val Max = "Max"
  val Min = "Min"

  def apply[A: Field: Order: ClassTag](data: Array[A]): Option[Summary[A]] =
    if (data.size > 0) {
      val mid = data.size / 2
      data.qselect(mid)
      val median0 = data(mid)
      val median = if (data.size % 2 == 0) {
        data.qselect(mid - 1)
        (median0 + data(mid - 1)) / 2
      } else median0
      Some(Summary(data.qmean, median, data.qmin, data.qmax))
    } else {
      None
    }

  def reducer[A: Field: Order: ClassTag] = Reducer[A, Summary[A]] { data =>
    Cell.fromOption(Summary(data))
  }
}
