package framian
package stats

import org.apache.commons.math3.linear.{ ArrayRealVector, Array2DRowRealMatrix }
import org.apache.commons.math3.linear.{ CholeskyDecomposition, NonPositiveDefiniteMatrixException }

import scala.reflect.ClassTag

import scala.collection.mutable.ArrayBuilder

import spire.algebra._
import spire.implicits._

case class OLS[Col](
    variables: Seq[Col],
    regression: LinearRegression[Double]) {

  def apply[Row](frame: Frame[Row, Col]): Series[Row, Double] = {
    val data = frame.get(Cols.unsized(variables).as(RowExtractor.denseCollectionOf[Array, Double, Col]))
    data.mapValues(regression(_))
  }

  def apply[V: Field: ClassTag](series: Series[Col, V]): Cell[V] =
    series.select(variables).map(_.toArray).map(regression.map(Field[V].fromDouble).apply(_))
}

object OLS {
  def apply[Row, Col](frame: Frame[Row, Col], dependent: Col, independents: Col*): OLS[Col] = {
    val dep = frame.column[Double](dependent)
    val ind = frame.get(Cols.unsized(independents).as(RowExtractor.denseCollectionOf[Array, Double, Col]))

    val indBldr = ArrayBuilder.make[Array[Double]]
    val depBldr = ArrayBuilder.make[Double]
    (ind zip dep).denseIterator.foreach { case (_, (i, d)) =>
      indBldr += i
      depBldr += d
    }

    val xss = indBldr.result().map(_ :+ 1D) // Ugh... make this better.
    val ys = depBldr.result()

    val m = new Array2DRowRealMatrix(xss, false)
    val m0 = m.transpose()
    val m1 = m0.multiply(m)
    val b = new ArrayRealVector(ys, false)
    val b0 = m0.operate(b)

    val soln = try {
      val decomp = new CholeskyDecomposition(m1)
      decomp.getSolver().solve(b0).toArray
    } catch {
      case (e: NonPositiveDefiniteMatrixException) =>
        // Not full-rank! Can solve using other method.
        throw e
    }

    val beta = soln.init
    val alpha = soln.last
    val regression = LinearRegression(beta, alpha)
    OLS(independents, regression)
  }
}
