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
  import Prop.{ classify, collect, forAll }
  import SeriesGenerators.{
    genEmptyArbitrarySeries,
    genNonEmptyArbitraryDenseSeries,
    genNonEmptyArbitraryDirtySeries,
    genNonEmptyArbitrarySparseSeries,
    genNonEmptyDenseSeries
  }

  implicit val params = Parameters(minTestsOk = 20)

  def classifySparse[A](c: TraversableOnce[Cell[A]])(prop: Prop): Prop =
    classify(c.exists(_.isNonValue), "sparse", "dense")(prop)

  def classifyMeaningful[A](c: TraversableOnce[Cell[A]])(prop: Prop): Prop =
    classify(c.exists(_ == NM), "meaningless", "meaningful")(prop)

  def reducingEmptySeriesMustEqNA[I: Arbitrary: ClassTag, O: ClassTag](reducer: Reducer[I, O]): Prop =
    forAll (genEmptyArbitrarySeries[Int, I]) { series =>
      collect(series.size) {
        series.reduce(reducer) must_== NA
      }
    } .set(minTestsOk = 10)

  def reducingMeaninglessSeriesMustEqNM[I: Arbitrary: ClassTag, O: ClassTag](reducer: Reducer[I, O]): Prop =
    forAll (genNonEmptyArbitraryDirtySeries[Int, I]()) { series =>
      classifyMeaningful(series.values) {
        series.values.exists(_.isNonValue) ==> (
          series.reduce(reducer) must_== NM
          )
      }
    }.set(minTestsOk = 10)

  "Count" should {

    "return 0 for an empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Int]) { series =>
        collect(series.size) {
          series.reduce(Count) must_== Value(0)
        }
      }.set(minTestsOk = 10)
    }

    "return the count for a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        series.reduce(Count) must_== Value(series.size)
      }
    }

    "return the count for a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.reduce(Count) must_==
            Value(series.values.count(_.isValue))
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM[Int, Int](Count)
  }


  "First" should {

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(First[Int])

    "return the first value in a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        series.reduce(First[Int]) must_== series.values.head
      }
    }

    "return the first value in a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.values.exists(_.isValue) ==> (
            series.reduce(First[Int]) must_== series.values.filter(_.isValue).head
            )
        }
      } and
        forAll (genNonEmptyArbitraryDirtySeries[Int, Int]()) { series =>
          classifyMeaningful(series.values) {
            series.reduce(First[Int]) must_== series.values.filterNot(_ == NA).head
          }
        }
    }
  }


  "FirstN" should {

    "return NA for an empty series" in {
      forAll (Arbitrary.arbitrary[Int]) { n =>
        (n > 0) ==> reducingEmptySeriesMustEqNA(FirstN[Int](n))
      }
    }

    "return the first n values in a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        forAll (Gen.choose(1, series.size)) { n =>
          series.reduce(FirstN[Int](n)) must_== Value(series.values.map(_.get).take(n))
        }
      }
    }

    "return the first n values in a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          forAll (Gen.choose(1, series.size)) { n =>
            val res = series.reduce(FirstN[Int](n))

            (n <= series.values.count(_.isValue)) ==> (
              res must_== Value(series.values.filter(_.isValue).map(_.get).take(n))
              ) ||
              (n > series.values.count(_.isValue)) ==> (
                res must_== NA
                )
          }
        }
      } and
        forAll (genNonEmptyArbitraryDirtySeries[Int, Int]()) { series =>
          classifyMeaningful(series.values) {
            forAll (Gen.choose(1, series.values.count(_.isValue))) { n =>
              val res = series.reduce(FirstN[Int](n))
              val containsNM = series.values.filterNot(_ == NA).take(n).contains(NM)

              containsNM ==> (
                res must_== NM
                ) ||
                !containsNM ==> (
                  res must_== Value(series.values.filter(_.isValue).map(_.get).take(n))
                  )
            }
          }
        }
    }
  }


  "Last" should {

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(Last[Int])

    "return the first value in a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        series.reduce(Last[Int]) must_== series.values.last
      }
    }

    "return the last value in a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.values.exists(_.isValue) ==> (
            series.reduce(Last[Int]) must_== series.values.filter(_.isValue).last
            )
        }
      } and
        forAll (genNonEmptyArbitraryDirtySeries[Int, Int]()) { series =>
          classifyMeaningful(series.values) {
            series.reduce(Last[Int]) must_== series.values.filterNot(_ == NA).last
          }
        }
    }
  }


  "LastN" should {

    "return NA for an empty series" in {
      forAll (Arbitrary.arbitrary[Int]) { n =>
        (n > 0) ==> reducingEmptySeriesMustEqNA(LastN[Int](n))
      }
    }

    "return the first n values in a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        forAll (Gen.choose(1, series.size)) { n =>
          series.reduce(LastN[Int](n)) must_== Value(series.values.map(_.get).takeRight(n))
        }
      }
    }

    "return the first n values in a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          val s = series

          forAll (Gen.choose(1, series.size)) { n =>
            val res = s.reduce(LastN[Int](n))

            (n <= series.values.count(_.isValue)) ==> (
              res must_== Value(series.values.filter(_.isValue).map(_.get).takeRight(n))
              ) ||
              (n > series.values.count(_.isValue)) ==> (
                res must_== NA
                )
          }
        }
      } and
        forAll (genNonEmptyArbitraryDirtySeries[Int, Int]()) { series =>
          classifyMeaningful(series.values) {
            val s = series

            forAll (Gen.choose(1, series.values.count(_.isValue))) { n =>
              val res = s.reduce(LastN[Int](n))
              val containsNM = series.values.filterNot(_ == NA).takeRight(n).contains(NM)

              containsNM ==> (
                res must_== NM
                ) ||
                !containsNM ==> (
                  res must_== Value(series.values.filter(_.isValue).map(_.get).takeRight(n))
                  )
            }
          }
        }
    }
  }


  "Max" should {

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(Max[Int])

    "return the max value in a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        series.reduce(Max[Int]) must_== Value(series.values.map(_.get).max)
      }
    }

    "return the max value in a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.values.exists(_.isValue) ==> (
            series.reduce(Max[Int]) must_==
              Value(series.values.filter(_.isValue).map(_.get).max)
            )
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Max[Int])
  }


  "Mean" should {

    "return NA for an empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Double]) { series =>
        collect(series.size) {
          series.reduce(Mean[Double]) must_== NA
        }
      }.set(minTestsOk = 10)
    }

    "return the mean value of a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Double]) { series =>
        series.reduce(Mean[Double]) must_== Value(series.values.map(_.get).sum / series.size)
      }
    }

    "return the mean value of a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Double]()) { series =>
        classifySparse(series.values) {
          series.values.exists(_.isValue) ==> (
            series.reduce(Mean[Double]) must_== {
              val l0 = series.values.filter(_.isValue).map(_.get)
              Value(l0.sum / l0.length)
            }
          )
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Mean[Double])
  }


  "Monoid" should {
    import spire.algebra.Monoid

    implicit val m = Monoid.additive[Int]

    "return monoid identity for an empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Int]) { series =>
        collect(series.size) {
          {
            implicit val m = Monoid.additive[Int]
            series.reduce(MonoidReducer[Int]) must_== Value(m.id)
          } and {
            implicit val m = Monoid.multiplicative[Int]
            series.reduce(MonoidReducer[Int]) must_== Value(m.id)
          }
        }
      }.set(minTestsOk = 10)
    }

    "return the monoidal reduction of a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series => {
        implicit val m = Monoid.additive[Int]
        series.reduce(MonoidReducer[Int]) must_== Value(series.values.map(_.get).sum)
      } and {
        implicit val m = Monoid.multiplicative[Int]
        series.reduce(MonoidReducer[Int]) must_== Value(series.values.map(_.get).product)
      }
      }
    }

    "return the monoidal reduction of a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.reduce(MonoidReducer[Int]) must_== Value(series.values.filter(_.isValue).map(_.get).sum)
        }
      }
    }

    "return MM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(MonoidReducer[Int])
  }


  "SemiGroup" should {
    import spire.algebra.Semigroup

    implicit val g = Semigroup.additive[Int]

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(SemigroupReducer[Int])

    "return the semigroup reduction of a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series => {
        implicit val g = Semigroup.additive[Int]
        series.reduce(SemigroupReducer[Int]) must_== Value(series.values.map(_.get).sum)
      } and {
        implicit val g = Semigroup.multiplicative[Int]
        series.reduce(SemigroupReducer[Int]) must_== Value(series.values.map(_.get).product)
      }
      }
    }

    "return the semigroup reduction of a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          val res = series.reduce(SemigroupReducer[Int])
          series.values.exists(_.isValue) ==> (
            res must_== Value(series.values.filter(_.isValue).map(_.get).sum)
            ) ||
            series.values.forall(_ == NA) ==> (
              res must_== NA
              )
        }
      }
    }

    "return MM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(SemigroupReducer[Int])
  }


  "Unique" should {

    "return the empty set for an empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Int]) { series =>
        collect(series.size) {
          series.reduce(Unique[Int]) must_== Value(Set.empty)
        }
      }.set(minTestsOk = 10)
    }

    "return the unique values of a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        series.reduce(Unique[Int]) must_== Value(series.values.map(_.get).toSet)
      }
    }

    "return the unique values of a sparse series" in {
      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.reduce(Unique[Int]) must_== Value(series.values.filter(_.isValue).map(_.get).toSet)
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Unique[Int])
  }


  "Exists" should {

    "return false for an empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Int]) { series =>
        collect(series.size) {
          series.reduce(Exists[Int](_ => true)) must_== Value(false)
        }
      }.set(minTestsOk = 10)
    }

    "evaluate the predicate for a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        (
          series.reduce(Exists[Int](_ => false)) must_== Value(false)
          ) && {
          val p = (i: Int) => i % 10 == 0

          classify(series.values.map(_.get).exists(p), "exists=true", "exists=false") {
            series.reduce(Exists(p)) must_== Value(series.values.map(_.get).exists(p))
          }
        }
      }
    }

    "evaluate the predicate for a sparse series" in {
      val p = (i: Int) => i % 10 == 0

      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.reduce(Exists(p)) must_== Value(series.values.filter(_.isValue).map(_.get).exists(p))
        }
      } and
        forAll (genNonEmptyArbitraryDirtySeries[Int, Int]()) { series =>
          classifyMeaningful(series.values) {
            series.reduce(Exists(p)) must_== Value(series.values.filter(_.isValue).map(_.get).exists(p))
          }
        }
    }
  }


  "Quantile" should {
    "return NA for empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Double]) { series =>
        collect(series.size) {
          series.reduce(Quantile[Double](List(0.25, 0.5, 0.75))) must_== NA
        }
      }.set(minTestsOk = 10)
    }

    "return min value for 0p" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Double]) { series =>
        val min = series.values.map(_.get).min
        series.reduce(Quantile[Double](List(0.0))) must_== Value(List(0.0 -> min))
      }
    }

    "return max value for 1p" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Double]) { series =>
        val max = series.values.map(_.get).max
        series.reduce(Quantile[Double](List(1.0))) must_== Value(List(1.0 -> max))
      }
    }

    val quantiles = List(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)

    "never return percentile below min or above max" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Double]) { series =>
        val min = series.values.map(_.get).min
        val max = series.values.map(_.get).max
        val percentiles = series.reduce(Quantile[Double](quantiles))
        percentiles.value.get.forall { case (_, q) =>
          q >= min && q <= max
        }
      }
    }

    "percentiles split at appropriate mark" in {
      def genRational = arbitrary[Double].map(Rational(_))
      forAll (genNonEmptyDenseSeries(arbitrary[Int], genRational)) { series =>
        val percentiles = series.reduce(Quantile[Rational](quantiles))
        percentiles.value.get forall { case (p, q) =>
          val below = math.ceil(series.values.size * p)
          val above = math.ceil(series.values.size * (1 - p))
          series.values.map(_.get).count(_ < q) <= below && series.values.map(_.get).count(_ > q) <= above
        }
      }
    }
  }


  "ForAll" should {

    "return true for an empty series" in {
      forAll (genEmptyArbitrarySeries[Int, Int]) { series =>
        collect(series.size) {
          series.reduce(ForAll[Int](_ => false)) must_== Value(true)
        }
      }.set(minTestsOk = 10)
    }

    "evaluate the predicate for a dense series" in {
      forAll (genNonEmptyArbitraryDenseSeries[Int, Int]) { series =>
        (
          series.reduce(ForAll[Int](_ => false)) must_== Value(false)
          ) && {
          val p = (i: Int) => i >= 0

          classify(series.values.map(_.get).forall(p), "forall=true", "forall=false") {
            series.reduce(ForAll(p)) must_== Value(series.values.map(_.get).forall(p))
          }
        }
      }
    }

    "evaluate the predicate for a sparse series" in {
      val p = (i: Int) => i >= 0

      forAll (genNonEmptyArbitrarySparseSeries[Int, Int]()) { series =>
        classifySparse(series.values) {
          series.reduce(ForAll(p)) must_== Value(series.values.filter(_.isValue).map(_.get).forall(p))
        }
      } and
        forAll (genNonEmptyArbitraryDirtySeries[Int, Int]()) { series =>
          classifyMeaningful(series.values) {
            val res = series.reduce(ForAll(p))
            val containsNM = series.values.contains(NM)
            containsNM ==> (
              res must_== Value(false)
              ) ||
              !containsNM ==> (
                res must_== Value(series.values.filter(_.isValue).map(_.get).forall(p))
                )
          }
        }
      }
    }
  }
