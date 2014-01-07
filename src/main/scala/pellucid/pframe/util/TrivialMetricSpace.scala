package pellucid.pframe.util

import spire.algebra.{ Eq, MetricSpace }
import spire.syntax.eq._

final class TrivialMetricSpace[V: Eq] extends MetricSpace[V, Int] {
  def distance(x: V, y: V): Int = if (x === y) 0 else 1
}

object TrivialMetricSpace {
  def apply[V: Eq] = new TrivialMetricSpace[V]
}
