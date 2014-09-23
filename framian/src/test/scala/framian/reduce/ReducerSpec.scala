package framian
package reduce

import scala.reflect.ClassTag

import spire.math.Rational
import spire.std.double._
import spire.std.int._
import spire.std.string._

import org.specs2.ScalaCheck
import org.specs2.matcher.Parameters
import org.specs2.mutable._

import org.scalacheck.{ Arbitrary, Gen, Prop }

class ReducerSpec extends Specification with ScalaCheck with SeriesClassifiers {
  import Arbitrary.arbitrary
  import Prop.{classify, collect, forAll}
  import SeriesGenerators._

  implicit val arbRational = Arbitrary(arbitrary[Double].map(Rational(_)))
  implicit val params = Parameters(minTestsOk = 20, maxDiscardRatio = 20F)

  object unique {
    val dense = Series("a" -> 1D, "b" -> 2D, "c" -> 4D, "d" -> 5D)
    val sparse = Series(Index.fromKeys("a", "b", "c", "d", "e", "f"),
      Column(NA, Value(2D), NM, NA, Value(4D), NM))
  }

  object odd {
    val dense = Series("a" -> 1D, "b" -> 2D, "c" -> 3D)
    val sparse = Series(Index.fromKeys("a", "b", "c", "d"),
      Column(NA, Value(2D), Value(4D), Value(5D)))
  }

  object duplicate {
    val dense = Series("a" -> 1D, "a" -> 2D, "b" -> 3D, "b" -> 4D, "b" -> 5D, "c" -> 6D)
    val sparse = Series.fromCells(
      "a" -> NA,
      "b" -> Value(2D), "b" -> NM, "b" -> NA, "b" -> Value(4D), "b" -> NM,
      "c" -> Value(5D), "c" -> NA, "c" -> Value(1D),
      "d" -> Value(0D))
  }

  def reducingMeaninglessSeriesMustEqNM[I: Arbitrary : ClassTag, O: ClassTag](reducer: Reducer[I, O]): Prop =
    forAll(arbitrary[Series[Int, I]]) { series =>
      classifyMeaningful(series) {
        (series.reduce(reducer) == NM) must_== series.cells.contains(NM)
      }
    }.set(minTestsOk = 10)


  "Count" should {

    "count dense series" in {
      unique.dense.reduce(Count) must_== Value(4)
      odd.dense.reduce(Count) must_== Value(3)
      duplicate.dense.reduce(Count) must_== Value(6)
    }

    "count sparse series" in {
      odd.sparse.reduce(Count) must_== Value(3)
    }

    "count dense series by key" in {
      duplicate.dense.reduceByKey(Count) must_== Series("a" -> 2, "b" -> 3, "c" -> 1)
    }

    "count sparse series by key" in {
      duplicate.sparse.reduceByKey(Count) must_==
        Series.fromCells("a" -> Value(0), "b" -> NM, "c" -> Value(2), "d" -> Value(1))
    }

    "return the count for series" in {
      check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            series.reduce(Count) must_== Value(series.cells.count(_.isValue))
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM[Int, Int](Count)
  }


  "First" should {

    "get first value in dense series" in {
      unique.dense.reduce(First[Double]) must_== Value(1D)
      duplicate.dense.reduce(First[Double]) must_== Value(1D)
      odd.dense.reduce(First[Double]) must_== Value(1D)

      unique.dense.reduce(FirstN[Double](1)) must_== Value(List(1D))
      duplicate.dense.reduce(FirstN[Double](1)) must_== Value(List(1D))
      odd.dense.reduce(FirstN[Double](1)) must_== Value(List(1D))
    }

    "get first value of sparse series" in {
      unique.sparse.reduce(First[Double]) must_== Value(2D)
      duplicate.sparse.reduce(First[Double]) must_== Value(2D)
      odd.sparse.reduce(First[Double]) must_== Value(2D)

      unique.sparse.reduce(FirstN[Double](1)) must_== Value(List(2D))
      duplicate.sparse.reduce(FirstN[Double](1)) must_== Value(List(2D))
      odd.sparse.reduce(FirstN[Double](1)) must_== Value(List(2D))
    }

    "get first in dense series by key" in {
      duplicate.dense.reduceByKey(First[Double]) must_==
        Series("a" -> 1D, "b" -> 3D, "c" -> 6D)

      duplicate.dense.reduceByKey(FirstN[Double](1)) must_==
        Series("a" -> List(1D), "b" -> List(3D), "c" -> List(6D))
    }

    "get first in sparse series by key" in {
      duplicate.sparse.reduceByKey(First[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> Value(2D), "c" -> Value(5D), "d" -> Value(0D))

      duplicate.sparse.reduceByKey(FirstN[Double](1)) must_==
        Series.fromCells("a" -> NA, "b" -> Value(List(2D)), "c" -> Value(List(5D)), "d" -> Value(List(0D)))
    }

    "return the first value in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifySparse(series) {
          classifyMeaningful(series) {
            val reduction = series.reduce(First[Int])
            if (!series.cells.exists(_ != NA)) {
              reduction must_== NA
            } else {
              reduction must_== series.cells.filter(_ != NA).head
            }
          }
        }
      }
    }
  }


  "FirstN" should {

    "get first N values in dense series" in {
      unique.dense.reduce(FirstN[Double](3)) must_== Value(List(1D, 2D, 4D))
      duplicate.dense.reduce(FirstN[Double](3)) must_== Value(List(1D, 2D, 3D))
      odd.dense.reduce(FirstN[Double](3)) must_== Value(List(1D, 2D, 3D))
    }

    "get first N values in sparse series" in {
      odd.sparse.reduce(FirstN[Double](3)) must_== Value(List(2D, 4D, 5D))
    }

    "get first N values in dense series by key" in {
      duplicate.dense.reduceByKey(FirstN[Double](2)) must_==
        Series.fromCells("a" -> Value(List(1D, 2D)), "b" -> Value(List(3D, 4D)), "c" -> NA)

      duplicate.dense.reduceByKey(FirstN[Double](3)) must_==
        Series.fromCells("a" -> NA, "b" -> Value(List(3D, 4D, 5D)), "c" -> NA)
    }

    "get first N values in sparse series by key" in {
      duplicate.sparse.reduceByKey(FirstN[Double](2)) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(5D, 1D)), "d" -> NA)
    }

    "return the first n values in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              forAll(Gen.choose(1, series.size)) { n =>
                val takeN = series.cells.filter(_ != NA).take(n)
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

    "get last value in dense series" in {
      unique.dense.reduce(Last[Double]) must_== Value(5D)
      duplicate.dense.reduce(Last[Double]) must_== Value(6D)
      odd.dense.reduce(Last[Double]) must_== Value(3D)

      unique.dense.reduce(LastN[Double](1)) must_== Value(List(5D))
      duplicate.dense.reduce(LastN[Double](1)) must_== Value(List(6D))
      odd.dense.reduce(LastN[Double](1)) must_== Value(List(3D))
    }

    "get last value of sparse series" in {
      duplicate.sparse.reduce(Last[Double]) must_== Value(0D)
      odd.sparse.reduce(Last[Double]) must_== Value(5D)

      duplicate.sparse.reduce(LastN[Double](1)) must_== Value(List(0D))
      odd.sparse.reduce(LastN[Double](1)) must_== Value(List(5D))
    }

    "get last in dense series by key" in {
      duplicate.dense.reduceByKey(Last[Double]) must_==
        Series("a" -> 2D, "b" -> 5D, "c" -> 6D)

      duplicate.dense.reduceByKey(LastN[Double](1)) must_==
        Series("a" -> List(2D), "b" -> List(5D), "c" -> List(6D))
    }

    "get last in sparse series by key" in {
      duplicate.sparse.reduceByKey(Last[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(1D), "d" -> Value(0D))

      duplicate.sparse.reduceByKey(LastN[Double](1)) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(1D)), "d" -> Value(List(0D)))
    }

    "return the last value in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              val reduction = series.reduce(Last[Int])
              if (!series.cells.exists(_ != NA)) {
                reduction must_== NA
              } else {
                reduction must_== series.cells.filter(_ != NA).last
              }
            }
          }
        }
      }
    }
  }


  "LastN" should {

    "get last N values in dense series" in {
      unique.dense.reduce(LastN[Double](3)) must_== Value(List(2D, 4D, 5D))
      duplicate.dense.reduce(LastN[Double](3)) must_== Value(List(4D, 5D, 6D))
      odd.dense.reduce(LastN[Double](3)) must_== Value(List(1D, 2D, 3D))
    }

    "get last N values in sparse series" in {
      odd.sparse.reduce(LastN[Double](3)) must_== Value(List(2D, 4D, 5D))
    }

    "get last N values in dense series by key" in {
      duplicate.dense.reduceByKey(LastN[Double](2)) must_==
        Series.fromCells("a" -> Value(List(1D, 2D)), "b" -> Value(List(4D, 5D)), "c" -> NA)

      duplicate.dense.reduceByKey(LastN[Double](3)) must_==
        Series.fromCells("a" -> NA, "b" -> Value(List(3D, 4D, 5D)), "c" -> NA)
    }

    "get last N values in sparse series by key" in {
      duplicate.sparse.reduceByKey(LastN[Double](2)) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(5D, 1D)), "d" -> NA)
    }

    "return the last n values in a series" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              forAll(Gen.choose(1, series.size)) { n =>
                val reduction = series.reduce(LastN[Int](n))
                val takeN = series.cells.filter(_ != NA).takeRight(n)
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

    "find max in dense series" in {
      unique.dense.reduce(Max[Double]) must_== Value(5D)
      odd.dense.reduce(Max[Double]) must_== Value(3D)
      duplicate.dense.reduce(Max[Double]) must_== Value(6D)
    }

    "find max in sparse series" in {
      odd.sparse.reduce(Max[Double]) must_== Value(5D)
    }

    "find max in dense series by key" in {
      duplicate.dense.reduceByKey(Max[Double]) must_== Series("a" -> 2D, "b" -> 5D, "c" -> 6D)
    }

    "find max in sparse series by key" in {
      duplicate.sparse.reduceByKey(Max[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(5D), "d" -> Value(0D))
    }

    "return the max value of a series" in {
      check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              series.reduce(Max[Int]) must_== NA
            } else {
              series.reduce(Max[Int]) must_== Value(series.values.max)
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Max[Int])
  }


  "Mean" should {

    "find mean of dense series" in {
      unique.dense.reduce(Mean[Double]) must_== Value(3D)
      duplicate.dense.reduce(Mean[Double]) must_== Value(3.5)
    }

    "find mean of sparse series" in {
      odd.sparse.reduce(Mean[Double]) must_== Value(11D / 3D)
    }

    "find mean of dense series by key" in {
      duplicate.dense.reduceByKey(Mean[Double]) must_==
        Series("a" -> 1.5, "b" -> 4D, "c" -> 6D)
    }

    "find mean of sparse series by key" in {
      duplicate.sparse.reduceByKey(Mean[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(3D), "d" -> Value(0D))
    }

    "return the mean value of a series" in {
      check1[MeaningfulSeries[Int, Double], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              series.reduce(Mean[Double]) must_== NA
            } else {
              series.reduce(Mean[Double]) must_== Value(series.values.sum / series.values.length)
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Mean[Double])
  }

  "Median" should {

    "find median in dense series" in {
      unique.dense.reduce(Median[Double]) must_== Value(3D)
      duplicate.dense.reduce(Median[Double]) must_== Value(3.5D)
      odd.dense.reduce(Median[Double]) must_== Value(2D)
    }

    "find median in sparse series" in {
      odd.sparse.reduce(Median[Double]) must_== Value(4D)
    }

    "find median in dense series by key" in {
      duplicate.dense.reduceByKey(Median[Double]) must_== Series("a" -> 1.5D, "b" -> 4D, "c" -> 6D)
    }

    "find median in sparse series by key" in {
      duplicate.sparse.reduceByKey(Median[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(3D), "d" -> Value(0D))
    }

    "return the median value of a series" in {
      check1[MeaningfulSeries[Int, Double], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            val dense = series.values.sorted
            if (dense.isEmpty) {
              series.reduce(Median[Double]) must_== NA
            } else {
              val dense = series.values.sorted
              val l = (dense.size - 1) / 2
              val u = dense.size / 2
              val median = (dense(l) + dense(u)) / 2
              series.reduce(Median[Double]) must_== Value(median)
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Median[Double])
  }


  "Monoid" should {
    import spire.algebra.Monoid

    "sum dense series with additive monoid" in {
      unique.dense.reduce(Sum[Double]) must_== Value(12D)
      duplicate.dense.reduce(Sum[Double]) must_== Value(21D)
    }

    "sum sparse series with additive monoid" in {
      odd.sparse.reduce(Sum[Double]) must_== Value(11D)
    }

    "sum dense series by key with additive monoid" in {
      duplicate.dense.reduceByKey(Sum[Double]) must_== Series("a" -> 3D, "b" -> 12D, "c" -> 6D)
    }

    "sum sparse series by key with additive monoid" in {
      duplicate.sparse.reduceByKey(Sum[Double]) must_== Series("a" -> 0D, "b" -> NM, "c" -> 6D, "d" -> 0D)
    }

    "return the monoidal reduction for a series" in {
      check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
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
                series.reduce(MonoidReducer[Int]) must_== Value(series.values.sum)
              } and {
                implicit val m = Monoid.multiplicative[Int]
                series.reduce(MonoidReducer[Int]) must_== Value(series.values.product)
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

    "reduce by key" in {
      val s = Series.fromCells(
        1 -> Value("a"), 1 -> Value("b"),
        2 -> NA, 2 -> Value("c"),
        3 -> Value("d"), 3 -> NA, 3 -> Value("e"), 3 -> NA, 3 -> Value("e"),
        4 -> NA, 4 -> NA)
      s.reduceByKey(SemigroupReducer[String]) must_==
        Series.fromCells(1 -> Value("ab"), 2 -> Value("c"), 3 -> Value("dee"), 4 -> NA)
    }

    "return the semi-group reduction of a series" in {
      check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
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
                series.reduce(SemigroupReducer[Int]) must_== Value(series.values.sum)
              } and {
                implicit val m = Semigroup.multiplicative[Int]
                series.reduce(SemigroupReducer[Int]) must_== Value(series.values.product)
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

    "return unique elements from dense series" in {
      unique   .dense.reduce(Unique[Double]) must_== Value(Set(1D, 2D, 4D, 5D))
      odd      .dense.reduce(Unique[Double]) must_== Value(Set(1D, 2D, 3D))
      duplicate.dense.reduce(Unique[Double]) must_== Value(Set(1D, 2D, 3D, 4D, 5D, 6D))

      Series(1 -> 1, 2 -> 1, 3 -> 2, 4 -> 1, 5 -> 3, 6 -> 2).reduce(Unique[Int]) must_== Value(Set(1, 2, 3))
    }

    "return unique elements in sparse series" in {
      odd.sparse.reduce(Unique[Double]) must_== Value(Set(2D, 4D, 5D))

      val s = Series.fromCells(1 -> Value("a"), 2 -> NA, 1 -> Value("b"), 3 -> NA)
      s.reduce(Unique[String]) must_== Value(Set("a", "b"))
    }

    "return unique elements from dense series by key" in {
      duplicate.dense.reduceByKey(Unique[Double]) must_==
        Series("a" -> Set(1D, 2D), "b" -> Set(3D, 4D, 5D), "c" -> Set(6D))
    }

    "return unique elements from sparse series by key" in {
      duplicate.sparse.reduceByKey(Unique[Double]) must_==
        Series.fromCells("a" -> Value(Set.empty), "b" -> NM, "c" -> Value(Set(1D, 5D)), "d" -> Value(Set(0D)))

      val s = Series.fromCells(
        1 -> Value("a"), 1 -> Value("b"),
        2 -> NA, 2 -> Value("c"),
        3 -> Value("d"), 3 -> NA, 3 -> Value("e"), 3 -> NA, 3 -> Value("e"),
        4 -> NA, 4 -> NA)
      s.reduceByKey(Unique[String]) must_== Series(1 -> Set("a", "b"), 2 -> Set("c"), 3 -> Set("d", "e"), 4 -> Set.empty)
    }

    "return the unique values for a series" in {
      check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              series.reduce(Unique[Int]) must_== Value(Set.empty)
            } else {
              series.reduce(Unique[Int]) must_== Value(series.values.toSet)
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

    "existentially quantify predicate over a dense series" in {
      unique.dense.reduce(Exists[Double](_ => true))  must_== Value(true)
      unique.dense.reduce(Exists[Double](_ => false)) must_== Value(false)
      unique.dense.reduce(Exists[Double](d => d < 2D)) must_== Value(true)
    }

    "existentially quantify predicate over sparse series" in {
      unique   .sparse.reduce(Exists[Double](d => d < 3D)) must_== Value(true)
      odd      .sparse.reduce(Exists[Double](d => d < 3D)) must_== Value(true)
      duplicate.sparse.reduce(Exists[Double](d => d < 3D)) must_== Value(true)
    }

    "existentially quantify predicate over a dense series by key" in {
      duplicate.dense.reduceByKey(Exists[Double](d => d < 2D)) must_==
        Series("a" -> true, "b" -> false, "c" -> false)
    }

    "existentially quantify predicate over sparse series by key" in {
      duplicate.sparse.reduceByKey(Exists[Double](d => d < 2D)) must_==
        Series("a" -> false, "b" -> false, "c" -> true, "d" -> true)
    }

    "return whether or not a predicate exists for a series" in {
      check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              series.reduce(Exists[Int](pAll)) must_== Value(false)
            } else {
              classify(series.values.exists(pMod10), "exists=true", "exists=false") {
                series.reduce(Exists(pNone)) must_== Value(false)
                series.reduce(Exists(pMod10)) must_== Value(series.values.exists(pMod10))
              }
            }
          }
        }
      }
    }
  }


  "Quantile" should {

    "return NA for empty series" in {
      check1[EmptySeries[Int, Rational], Prop] { case EmptySeries(series) =>
        collect(series.size) {
          series.reduce(Quantile[Rational](List(0.25, 0.5, 0.75))) must_== NA
        }
      }.set(minTestsOk = 10)
    }

    "return min value for 0p" in {
      forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          val min = series.values.min
          series.reduce(Quantile[Rational](List(0.0))) must_== Value(List(0.0 -> min))
        }
      }
    }

    "return max value for 1p" in {
      forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          val max = series.values.max
          series.reduce(Quantile[Rational](List(1.0))) must_== Value(List(1.0 -> max))
        }
      }
    }

    "never return percentile below min or above max" in {
      forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
          classifySparse(series) {
            val min = series.values.min
            val max = series.values.max
            series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (_, q) =>
              q must be >= min
              q must be <= max
            }
          }
        }
      }
    }

    "percentiles split at appropriate mark" in {
      forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
          series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (p, q) =>
            val below = math.ceil(series.values.size * p)
            val above = math.ceil(series.values.size * (1 - p))
            series.values.count(_ < q) <= below && series.values.count(_ > q) <= above
          }
        }
      }
    }
  }


  "ForAll" should {

    val pTrue = (_ => true): Int => Boolean
    val pFalse = (_ => false): Int => Boolean
    val pPositive = (_ > 0): Int => Boolean

    "universally quantify predicate over a dense series" in {
      unique.dense.reduce(ForAll[Double](_ => true))   must_== Value(true)
      unique.dense.reduce(ForAll[Double](_ => false))  must_== Value(false)
      unique.dense.reduce(ForAll[Double](d => d > 0D)) must_== Value(true)
    }

    "universally quantify predicate over a sparse series with only unavailablity" in {
      odd.sparse.reduce(ForAll[Double](d => d > 0D)) must_== Value(true)
    }

    "universally quantify predicate over a sparse series with not meaningful values" in {
      unique   .sparse.reduce(ForAll[Double](_ => true))  must_== Value(false)
      unique   .sparse.reduce(ForAll[Double](_ => false)) must_== Value(false)
      duplicate.sparse.reduce(ForAll[Double](_ => true))  must_== Value(false)
      duplicate.sparse.reduce(ForAll[Double](_ => false)) must_== Value(false)
    }

    "universally quantify predicate over a dense series by key" in {
      duplicate.dense.reduceByKey(ForAll[Double](_ => false)) must_==
        Series("a" -> false, "b" -> false, "c" -> false)
      duplicate.dense.reduceByKey(ForAll[Double](_ => true)) must_==
        Series("a" -> true, "b" -> true, "c" -> true)
      duplicate.dense.reduceByKey(ForAll[Double](d => d < 6D)) must_==
        Series("a" -> true, "b" -> true, "c" -> false)
    }

    "universally quantify predicate over a sparse series by key" in {
      duplicate.sparse.reduceByKey(ForAll[Double](_ => false)) must_==
        Series("a" -> true, "b" -> false, "c" -> false, "d" -> false)
      duplicate.sparse.reduceByKey(ForAll[Double](_ => true)) must_==
        Series("a" -> true, "b" -> false, "c" -> true, "d" -> true)
      duplicate.sparse.reduceByKey(ForAll[Double](d => d < 5D)) must_==
        Series("a" -> true, "b" -> false, "c" -> false, "d" -> true)
    }

    "return true for an empty series" in {
      check1[EmptySeries[Int, Int], Prop] { case EmptySeries(series) =>
        collect(series.size) {
          series.reduce(ForAll[Int](pFalse)) must_== Value(true)
        }
      }.set(minTestsOk = 10)
    }

    "return false for a series that contains NM" in {
      forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyMeaningful(series) {
          series.reduce(ForAll[Int](pTrue)) must_== Value(!series.cells.contains(NM))
        }
      }
    }

    "evaluate the predicate for a series" in {
      forAll(arbitrary[MeaningfulSeries[Int, Int]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          classify(series.values.min > 0, "forall=true", "forall=false") {
            series.reduce(ForAll[Int](pPositive)) must_== Value(series.values.min > 0)
          }
        }
      }
    }
  }
}
