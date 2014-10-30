package framian
package stats

import scala.reflect.ClassTag

import spire.algebra._
import spire.math._
import spire.std.double._
import spire.std.string._

final case class Statistic(name: String) {
  override def toString: String = name
}

object Statistic {
  val count = Statistic("count")
  val mean = Statistic("mean")
  val median = Statistic("median")
  val min = Statistic("min")
  val max = Statistic("max")
  val standardDeviation = Statistic("std. dev.")
  val variance = Statistic("variance")

  implicit val StatisticOrder: Order[Statistic] = Order.by(_.name)
}

object ops {
  implicit class FrameStatsOps[Row, Col](self: Frame[Row, Col]) {
    import self.{ rowClassTag, colClassTag, rowOrder, colOrder }

    def summary: Frame[Col, Statistic] = {
      val s = self.reduceFrame(Summary.reducer[Number])
      Frame.mergeColumns(
        Statistic.mean -> s.mapValues(_.mean),
        Statistic.median -> s.mapValues(_.median),
        Statistic.min -> s.mapValues(_.min),
        Statistic.max -> s.mapValues(_.max)
      )
    }

    def stdDev: Frame[Col, Statistic] = {
      val s = self.reduceFrame(StdDev.reducer[Double])
      Frame.mergeColumns(
        Statistic.count -> s.mapValues(_.count.toDouble),
        Statistic.mean -> s.mapValues(_.mean),
        Statistic.standardDeviation -> s.mapValues(_.stdDev),
        Statistic.variance -> s.mapValues(_.variance)
      )
    }

    def standardDeviation: Series[Col, Double] =
      self.reduceFrame(StdDev.reducer[Double].map(_.stdDev))

    def variance: Series[Col, Double] =
      self.reduceFrame(StdDev.reducer[Double].map(_.variance))

    def ols(dependent: Col, independent0: Col, extraIndependent: Col*): OLS[Col] =
      OLS(self, dependent, independent0 +: extraIndependent: _*)
  }

  implicit class SeriesStatsOps[K, V](self: Series[K, V]) {
    def summary(implicit V0: Order[V], V1: Field[V], ct: ClassTag[V]): Cell[Summary[V]] =
      self.reduce(Summary.reducer)

    def stdDev(implicit V0: Field[V], V1: NRoot[V], ct: ClassTag[V]): Cell[StdDev[V]] =
      self.reduce(StdDev.reducer)

    def standardDeviation(implicit V0: Field[V], V1: NRoot[V], ct: ClassTag[V]): Cell[V] =
      self.reduce(StdDev.reducer).map(_.stdDev)

    def variance(implicit V0: Field[V], V1: NRoot[V], ct: ClassTag[V]): Cell[V] =
      self.reduce(StdDev.reducer).map(_.variance)
  }
}
