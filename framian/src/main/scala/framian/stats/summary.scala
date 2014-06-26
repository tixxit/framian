package framian
package stats

import scala.reflect.ClassTag

import spire.algebra._
import spire.math.Number
import spire.std.string._

object summary {
  val Mean: String = "Mean"
  val Median: String = "Median"
  val Max: String = "Max"
  val Min: String = "Min"

  // TODO: Provide way to choose "optimal" field type for frame row/col.

  def apply[Row, Col](f: Frame[Row, Col]): Frame[Col, String] = {
    import f.{ colClassTag, colOrder }

    Frame.fromSeries(
      Mean -> f.reduceFrame(reduce.Mean[Number]),
      Median -> f.reduceFrame(reduce.Median[Number]),
      Max -> f.reduceFrame(reduce.Max[Number]),
      Min -> f.reduceFrame(reduce.Min[Number]))
  }

  def apply[K, V: Field: Order: ClassTag](s: Series[K, V]): Series[String, V] =
    Series.fromCells(
      Mean -> s.reduce(reduce.Mean[V]),
      Median -> s.reduce(reduce.Median[V]),
      Max -> s.reduce(reduce.Max[V]),
      Min -> s.reduce(reduce.Min[V]))
}
