package framian
package stats

import scala.reflect.ClassTag

import spire.algebra._

object ops {
  implicit class FrameStatsOps[Row, Col](self: Frame[Row, Col]) {
    def summary: Frame[Col, String] =
      framian.stats.summary.frame(self)
  }

  implicit class SeriesStatsOps[K, V](self: Series[K, V]) {
    def summary(implicit V0: Order[V], V1: Field[V], ct: ClassTag[V]): Series[String, V] =
      framian.stats.summary.series(self)
  }
}
