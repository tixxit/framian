package framian
package reduce

import scala.reflect.ClassTag

import spire.math.Rational
import spire.std.int._
import spire.std.double._

import org.specs2.ScalaCheck
import org.specs2.matcher.Parameters
import org.specs2.mutable._

import org.scalacheck.{ Arbitrary, Gen, Prop }

class ReducerPropSpec extends Specification with ScalaCheck {
  import Arbitrary.arbitrary
  import Prop.{classify, collect, forAll}
  import SeriesGenerators.arbSeries

  implicit val params = Parameters(minTestsOk = 20, maxDiscardRatio = 20F)

  def classifySparse[K, V](s: Series[K, V])(prop: Prop): Prop =
    classify(s.values.exists(_.isNonValue), "sparse", "dense")(prop)

  def classifyMeaningful[K, V](s: Series[K, V])(prop: Prop): Prop =
    classify(s.values.contains(NM), "meaningless", "meaningful")(prop)

  def classifyEmpty[K, V](s: Series[K, V])(prop: Prop): Prop =
    classify(s.values.exists(_.isValue), "non-empty, empty")(prop)

  def reducingEmptySeriesMustEqNA[I: Arbitrary: ClassTag, O: ClassTag](reducer: Reducer[I, O]): Prop = {
    // Create series that only contain sparse entries
    forAll(arbitrary[Series[Int, I]]) { series =>
      classifyEmpty(series) {
        // If the series does not contain any Values, then the reducer should always result in NA
        (!series.values.exists(_.isValue)) must_== (series.reduce(reducer) == NA)
      }
    }.set(minTestsOk = 10)
  }

  def reducingMeaninglessSeriesMustEqNM[I: Arbitrary : ClassTag, O: ClassTag](reducer: Reducer[I, O]): Prop =
    forAll(arbitrary[Series[Int, I]]) { series =>
      classifyMeaningful(series) {
        (series.reduce(reducer) == NM) must_== series.values.contains(NM)
      }
    }.set(minTestsOk = 10)

  "Count" should {

    "return the count for series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            series.reduce(Count) must_== Value(series.values.count(_.isValue))
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM[Int, Int](Count)
  }


  "First" should {

    "return the first value in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifySparse(series) {
          classifyMeaningful(series) {
            val reduction = series.reduce(First[Int])
            if (series.values.filter(_ != NA).length == 0) {
              reduction must_== NA
            } else {
              reduction must_== series.values.filter(_ != NA).head
            }
          }
        }
      }
    }
  }


  "FirstN" should {

    "return the first n values in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              forAll(Gen.choose(1, series.size)) { n =>
                val takeN = series.values.filter(_ != NA).take(n)
                if (takeN.contains(NM)) {
                  // If the firstN contains an NM, the result must be NM
                  series.reduce(FirstN[Int](n)) must_== NM
                } else if (takeN.length < n) {
                  // If there are not enough valid values, the result must be NA
                  series.reduce(FirstN[Int](n)) must_== NA
                } else {
                  // Otherwise, we should have a valid range of only the valid Values
                  series.reduce(FirstN[Int](n)) must_== Value(takeN.map(_.get))
                }
              }
            }
          }
        }
      }
    }
  }


  "Last" should {

    "return the last value in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              val reduction = series.reduce(Last[Int])
              if (series.values.filter(_ != NA).length == 0) {
                reduction must_== NA
              } else {
                reduction must_== series.values.filter(_ != NA).last
              }
            }
          }
        }
      }
    }
  }


  "LastN" should {

    "return the last n values in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              forAll(Gen.choose(1, series.size)) { n =>
                val reduction = series.reduce(LastN[Int](n))
                val takeN = series.values.filter(_ != NA).takeRight(n)
                if (takeN.contains(NM)) {
                  // If the lastN contains an NM, the result must be NM
                  reduction must_== NM
                } else if (takeN.length < n) {
                  // If there is no valid Value, the result must be NA
                  reduction must_== NA
                } else {
                  // Otherwise, we should have a valid range of only the valid Values
                  reduction must_== Value(takeN.map(_.get))
                }
              }
            }
          }
        }
      }
    }
  }


  "Max" should {

    "return the max value of a series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.denseValues.isEmpty) {
              series.reduce(Max[Int]) must_== NA
            } else {
              series.reduce(Max[Int]) must_== Value(series.denseValues.max)
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Max[Int])
  }


  "Mean" should {

    "return the mean value of a series" in {
      forAll(arbitrary[Series[Int, Double]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.denseValues.isEmpty) {
              series.reduce(Mean[Double]) must_== NA
            } else {
              series.reduce(Mean[Double]) must_== Value(series.denseValues.sum / series.denseValues.length)
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Mean[Double])
  }


  "Monoid" should {
    import spire.algebra.Monoid

    "return the monoidal reduction for a series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.denseValues.isEmpty) {
              // For empty series, ensure the reducers return the identity value
              {
                implicit val m = Monoid.additive[Int]
                series.reduce(MonoidReducer[Int]) must_== Value(m.id)
              } and {
                implicit val m = Monoid.multiplicative[Int]
                series.reduce(MonoidReducer[Int]) must_== Value(m.id)
              }
            } else {
              // For non-empty series, ensure the reducers return the correct value
              {
                implicit val m = Monoid.additive[Int]
                series.reduce(MonoidReducer[Int]) must_== Value(series.denseValues.sum)
              } and {
                implicit val m = Monoid.multiplicative[Int]
                series.reduce(MonoidReducer[Int]) must_== Value(series.denseValues.product)
              }
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in {
      implicit val m = Monoid.additive[Int]
      reducingMeaninglessSeriesMustEqNM(MonoidReducer[Int])
    }
  }


  "SemiGroup" should {
    import spire.algebra.Semigroup

    "return the semigroup reduction of a series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.denseValues.isEmpty) {
              // For empty series, ensure the reducers return NA
              {
                implicit val g = Semigroup.additive[Int]
                series.reduce(SemigroupReducer[Int]) must_== NA
              } and {
                implicit val g = Semigroup.multiplicative[Int]
                series.reduce(SemigroupReducer[Int]) must_== NA
              }
            } else {
              // For non-empty series, ensure the reducers return the correct value
              {
                implicit val m = Semigroup.additive[Int]
                series.reduce(SemigroupReducer[Int]) must_== Value(series.denseValues.sum)
              } and {
                implicit val m = Semigroup.multiplicative[Int]
                series.reduce(SemigroupReducer[Int]) must_== Value(series.denseValues.product)
              }
            }
          }
        }
      }
    }

    implicit val m = Semigroup.additive[Int]
    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(SemigroupReducer[Int])
  }


  "Unique" should {

    "return the unique values for a series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.denseValues.isEmpty) {
              series.reduce(Unique[Int]) must_== Value(Set.empty)
            } else {
              series.reduce(Unique[Int]) must_== Value(series.denseValues.toSet)
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Unique[Int])
  }


  "Exists" should {

    val pAll = (_ => true): Int => Boolean
    val pNone = (_ => false): Int => Boolean
    val pMod10 = (_ % 10 == 0): Int => Boolean

    "return whether or not a predicate exists for a series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM))) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.denseValues.isEmpty) {
              series.reduce(Exists[Int](pAll)) must_== Value(false)
            } else {
              classify(series.denseValues.exists(pMod10), "exists=true", "exists=false") {
                series.reduce(Exists(pNone)) must_== Value(false)
                series.reduce(Exists(pMod10)) must_== Value(series.denseValues.exists(pMod10))
              }
            }
          }
        }
      }
    }
  }


  "Quantile" should {

    "return NA for empty series" in {
      forAll (SeriesGenerators.genSeries(arbitrary[Int], arbitrary[Double], (0, 1, 0))) { series =>
        collect(series.size) {
          series.reduce(Quantile[Double](List(0.25, 0.5, 0.75))) must_== NA
        }
      }.set(minTestsOk = 10)
    }

    "return min value for 0p" in {
      forAll(arbitrary[Series[Int, Double]].suchThat(!_.values.contains(NM)).suchThat(_.denseValues.nonEmpty)) { series =>
        classifySparse(series) {
          val min = series.denseValues.min
          series.reduce(Quantile[Double](List(0.0))) must_== Value(List(0.0 -> min))
        }
      }
    }

    "return max value for 1p" in {
      forAll(arbitrary[Series[Int, Double]].suchThat(!_.values.contains(NM)).suchThat(_.denseValues.nonEmpty)) { series =>
        classifySparse(series) {
          val max = series.denseValues.max
          series.reduce(Quantile[Double](List(1.0))) must_== Value(List(1.0 -> max))
        }
      }
    }

    "never return percentile below min or above max" in {
      forAll(arbitrary[Series[Int, Double]].suchThat(!_.values.contains(NM)).suchThat(_.denseValues.nonEmpty)) { series =>
        forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
          classifySparse(series) {
            val min = series.denseValues.min
            val max = series.denseValues.max
            series.reduce(Quantile[Double](quantiles)).value.get.forall { case (_, q) =>
              q >= min && q <= max
            }
          }
        }
      }
    }

    "percentiles split at appropriate mark" in {
      implicit val arbRational = Arbitrary(arbitrary[Double].map(Rational(_)))
      forAll(arbitrary[Series[Int, Rational]].suchThat(!_.values.contains(NM)).suchThat(_.denseValues.nonEmpty)) { series =>
        forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
          series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (p, q) =>
            val below = math.ceil(series.denseValues.size * p)
            val above = math.ceil(series.denseValues.size * (1 - p))
            series.denseValues.count(_ < q) <= below && series.denseValues.count(_ > q) <= above
          }
        }
      }
    }
  }


  "ForAll" should {

    val pTrue = (_ => true): Int => Boolean
    val pFalse = (_ => false): Int => Boolean
    val pPositive = (_ > 0): Int => Boolean

    "return true for an empty series" in {
      forAll(SeriesGenerators.genSeries(arbitrary[Int], arbitrary[Int], (0, 1, 0))) { series =>
        collect(series.size) {
          series.reduce(ForAll[Int](pFalse)) must_== Value(true)
        }
      }.set(minTestsOk = 10)
    }

    "return false for a series that contains NM" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyMeaningful(series) {
          series.reduce(ForAll[Int](pTrue)) must_== Value(!series.values.contains(NM))
        }
      }
    }

    "evaluate the predicate for a series" in {
      forAll(arbitrary[Series[Int, Int]].suchThat(!_.values.contains(NM)).suchThat(_.denseValues.nonEmpty)) { series =>
        classifySparse(series) {
          classify(series.denseValues.min > 0, "forall=true", "forall=false") {
            series.reduce(ForAll[Int](pPositive)) must_== Value(series.denseValues.min > 0)
          }
        }
      }
    }
  }
}