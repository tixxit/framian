package framian

import org.scalacheck.Prop
import org.scalacheck.Prop._

trait SeriesClassifiers {

  def classifySparse[K, V](s: Series[K, V])(prop: Prop): Prop =
    classify(s.cells.exists(_.isNonValue), "sparse", "dense")(prop)

  def classifyMeaningful[K, V](s: Series[K, V])(prop: Prop): Prop =
    classify(s.cells.contains(NM), "meaningless", "meaningful")(prop)

  def classifyEmpty[K, V](s: Series[K, V])(prop: Prop): Prop =
    classify(s.cells.exists(_.isValue), "non-empty, empty")(prop)

}
object SeriesClassifiers extends SeriesClassifiers

