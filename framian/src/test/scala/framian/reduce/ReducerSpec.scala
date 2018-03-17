package framian
package reduce

import scala.reflect.ClassTag
import spire.math.Rational
import spire.std.double._
import spire.std.bigDecimal._
import spire.std.int._
import spire.std.string._
import spire.syntax.std.seq._

import org.scalacheck.{ Arbitrary, Gen, Prop }

import org.scalatest.prop.Checkers

class ReducerSpec extends FramianSpec
  with Checkers
  with SeriesClassifiers {

  import Arbitrary.arbitrary
  import Prop.{classify, collect, forAll}
  import SeriesGenerators._

  implicit val arbRational = Arbitrary(arbitrary[Double].map(Rational(_)))
  private val minSuccessfulPassed = 20
  implicit val params = PropertyCheckConfig(minSuccessful = minSuccessfulPassed, maxDiscarded = minSuccessfulPassed * 20)

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

  def reducingMeaninglessSeriesMustEqNM[I: Arbitrary : ClassTag, O: ClassTag](reducer: Reducer[I, O]): Unit =
    check(
      (series: Series[Int, I]) => classifyMeaningful(series) {
        Prop((series.reduce(reducer) == NM) == series.cells.contains(NM))
      },
      minSuccessful(10))

  "Count" should {
    "count dense series" in {
      unique.dense.reduce(Count) should === (Value(4))
      odd.dense.reduce(Count) should === (Value(3))
      duplicate.dense.reduce(Count) should === (Value(6))
    }

    "count sparse series" in {
      odd.sparse.reduce(Count) should === (Value(3))
    }

    "count dense series by key" in {
      duplicate.dense.reduceByKey(Count) should === (Series("a" -> 2, "b" -> 3, "c" -> 1))
    }

    "count sparse series by key" in {
      duplicate.sparse.reduceByKey(Count) should === (
        Series.fromCells("a" -> Value(0), "b" -> NM, "c" -> Value(2), "d" -> Value(1)))
    }

    "return the count for series" in {
      check { m: MeaningfulSeries[Int, Int] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            Prop(m.series.reduce(Count) == Value(m.series.cells.count(_.isValue)))
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM[Int, Int](Count)
  }

  "First" should {
    "get first value in dense series" in {
      unique.dense.reduce(First[Double]) should === (Value(1D))
      duplicate.dense.reduce(First[Double]) should === (Value(1D))
      odd.dense.reduce(First[Double]) should === (Value(1D))

      unique.dense.reduce(FirstN[Double](1)) should === (Value(List(1D)))
      duplicate.dense.reduce(FirstN[Double](1)) should === (Value(List(1D)))
      odd.dense.reduce(FirstN[Double](1)) should === (Value(List(1D)))
    }

    "get first value of sparse series" in {
      unique.sparse.reduce(First[Double]) should === (Value(2D))
      duplicate.sparse.reduce(First[Double]) should === (Value(2D))
      odd.sparse.reduce(First[Double]) should === (Value(2D))

      unique.sparse.reduce(FirstN[Double](1)) should === (Value(List(2D)))
      duplicate.sparse.reduce(FirstN[Double](1)) should === (Value(List(2D)))
      odd.sparse.reduce(FirstN[Double](1)) should === (Value(List(2D)))
    }

    "get first in dense series by key" in {
      duplicate.dense.reduceByKey(First[Double]) should === (
        Series("a" -> 1D, "b" -> 3D, "c" -> 6D))

      duplicate.dense.reduceByKey(FirstN[Double](1)) should === (
        Series("a" -> List(1D), "b" -> List(3D), "c" -> List(6D)))
    }

    "get first in sparse series by key" in {
      duplicate.sparse.reduceByKey(First[Double]) should === (
        Series.fromCells("a" -> NA, "b" -> Value(2D), "c" -> Value(5D), "d" -> Value(0D)))

      duplicate.sparse.reduceByKey(FirstN[Double](1)) should === (
        Series.fromCells("a" -> NA, "b" -> Value(List(2D)), "c" -> Value(List(5D)), "d" -> Value(List(0D))))
    }

    "return the first value in a series" in {
      check { series: Series[Int, Int] =>
        classifySparse(series) {
          classifyMeaningful(series) {
            val reduction = series.reduce(First[Int])

            if (!series.cells.exists(_ != NA)) {
              Prop(reduction == (NA))
            } else {
              Prop(reduction == (series.cells.filter(_ != NA).head))
            }
          }
        }
      }
    }
  }

  "FirstN" should {
    "get first N values in dense series" in {
      unique.dense.reduce(FirstN[Double](3)) should === (Value(List(1D, 2D, 4D)))
      duplicate.dense.reduce(FirstN[Double](3)) should === (Value(List(1D, 2D, 3D)))
      odd.dense.reduce(FirstN[Double](3)) should === (Value(List(1D, 2D, 3D)))
    }

    "get first N values in sparse series" in {
      odd.sparse.reduce(FirstN[Double](3)) should === (Value(List(2D, 4D, 5D)))
    }

    "get first N values in dense series by key" in {
      duplicate.dense.reduceByKey(FirstN[Double](2)) should === (
        Series.fromCells("a" -> Value(List(1D, 2D)), "b" -> Value(List(3D, 4D)), "c" -> NA))

      duplicate.dense.reduceByKey(FirstN[Double](3)) should === (
        Series.fromCells("a" -> NA, "b" -> Value(List(3D, 4D, 5D)), "c" -> NA))
    }

    "get first N values in sparse series by key" in {
      duplicate.sparse.reduceByKey(FirstN[Double](2)) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(5D, 1D)), "d" -> NA))
    }

    "return the first n values in a series" in {
      forAll(arbitrary[MeaningfulSeries[Int, Int]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            forAll(Gen.choose(1, series.size)) { n =>
              val takeN = series.cells.filter(_ != NA).take(n)

              if (takeN.contains(NM)) {
                // If the firstN contains an NM, the result must be NM
                Prop(series.reduce(FirstN[Int](n)) == NM)
              } else if (takeN.length < n) {
                // If there are not enough valid values, the result must be NA
                Prop(series.reduce(FirstN[Int](n)) == NA)
              } else {
                // Otherwise, we should have a valid range of only the valid Values
                Prop(series.reduce(FirstN[Int](n)) == Value(takeN.map(_.get)))
              }
            }
          }
        }
      }
    }
  }

  "Last" should {
    "get last value in dense series" in {
      unique.dense.reduce(Last[Double]) should === (Value(5D))
      duplicate.dense.reduce(Last[Double]) should === (Value(6D))
      odd.dense.reduce(Last[Double]) should === (Value(3D))

      unique.dense.reduce(LastN[Double](1)) should === (Value(List(5D)))
      duplicate.dense.reduce(LastN[Double](1)) should === (Value(List(6D)))
      odd.dense.reduce(LastN[Double](1)) should === (Value(List(3D)))
    }

    "get last value of sparse series" in {
      duplicate.sparse.reduce(Last[Double]) should === (Value(0D))
      odd.sparse.reduce(Last[Double]) should === (Value(5D))

      duplicate.sparse.reduce(LastN[Double](1)) should === (Value(List(0D)))
      odd.sparse.reduce(LastN[Double](1)) should === (Value(List(5D)))
    }

    "get last in dense series by key" in {
      duplicate.dense.reduceByKey(Last[Double]) should === (
        Series("a" -> 2D, "b" -> 5D, "c" -> 6D))

      duplicate.dense.reduceByKey(LastN[Double](1)) should === (
        Series("a" -> List(2D), "b" -> List(5D), "c" -> List(6D)))
    }

    "get last in sparse series by key" in {
      duplicate.sparse.reduceByKey(Last[Double]) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(1D), "d" -> Value(0D)))

      duplicate.sparse.reduceByKey(LastN[Double](1)) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(1D)), "d" -> Value(List(0D))))
    }

    "return the last value in a series" in {
      check { series: Series[Int, Int] =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              val reduction = series.reduce(Last[Int])

              if (!series.cells.exists(_ != NA)) {
                Prop(reduction == NA)
              } else {
                Prop(reduction == (series.cells.filter(_ != NA).last))
              }
            }
          }
        }
      }
    }
  }

  "LastN" should {
    "get last N values in dense series" in {
      unique.dense.reduce(LastN[Double](3)) should === (Value(List(2D, 4D, 5D)))
      duplicate.dense.reduce(LastN[Double](3)) should === (Value(List(4D, 5D, 6D)))
      odd.dense.reduce(LastN[Double](3)) should === (Value(List(1D, 2D, 3D)))
    }

    "get last N values in sparse series" in {
      odd.sparse.reduce(LastN[Double](3)) should === (Value(List(2D, 4D, 5D)))
    }

    "get last N values in dense series by key" in {
      duplicate.dense.reduceByKey(LastN[Double](2)) should === (
        Series.fromCells("a" -> Value(List(1D, 2D)), "b" -> Value(List(4D, 5D)), "c" -> NA))

      duplicate.dense.reduceByKey(LastN[Double](3)) should === (
        Series.fromCells("a" -> NA, "b" -> Value(List(3D, 4D, 5D)), "c" -> NA))
    }

    "get last N values in sparse series by key" in {
      duplicate.sparse.reduceByKey(LastN[Double](2)) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(5D, 1D)), "d" -> NA))
    }

    "return the last n values in a series" in {
      forAll(arbitrary[MeaningfulSeries[Int, Int]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          forAll(Gen.choose(1, series.size)) { n =>
            val reduction = series.reduce(LastN[Int](n))
            val takeN = series.cells.filter(_ != NA).takeRight(n)

            if (takeN.contains(NM)) {
              // If the lastN contains an NM, the result must be NM
              Prop(reduction == NM)
            } else if (takeN.length < n) {
              // If there is no valid Value, the result must be NA
              Prop(reduction == NA)
            } else {
              // Otherwise, we should have a valid range of only the valid Values
              Prop(reduction == Value(takeN.map(_.get)))
            }
          }
        }
      }
    }
  }

  "Max" should {
    "find max in dense series" in {
      unique.dense.reduce(Max[Double]) should === (Value(5D))
      odd.dense.reduce(Max[Double]) should === (Value(3D))
      duplicate.dense.reduce(Max[Double]) should === (Value(6D))
    }

    "find max in sparse series" in {
      odd.sparse.reduce(Max[Double]) should === (Value(5D))
    }

    "find max in dense series by key" in {
      duplicate.dense.reduceByKey(Max[Double]) should === (Series("a" -> 2D, "b" -> 5D, "c" -> 6D))
    }

    "find max in sparse series by key" in {
      duplicate.sparse.reduceByKey(Max[Double]) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(5D), "d" -> Value(0D)))
    }

    "return the max value of a series" in {
      check { m: MeaningfulSeries[Int, Int] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            if (m.series.values.isEmpty) {
              Prop(m.series.reduce(Max[Int]) == NA)
            } else {
              Prop(m.series.reduce(Max[Int]) == Value(m.series.values.max))
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Max[Int])
  }

  "Mean" should {
    "find mean of dense series" in {
      unique.dense.reduce(Mean[Double]) should === (Value(3D))
      duplicate.dense.reduce(Mean[Double]) should === (Value(3.5))
    }

    "find mean of sparse series" in {
      odd.sparse.reduce(Mean[Double]) should === (Value(11D / 3D))
    }

    "find mean of dense series by key" in {
      duplicate.dense.reduceByKey(Mean[Double]) should === (
        Series("a" -> 1.5, "b" -> 4D, "c" -> 6D))
    }

    "find mean of sparse series by key" in {
      duplicate.sparse.reduceByKey(Mean[Double]) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(3D), "d" -> Value(0D)))
    }

    "return the mean value of a series" in {
      check { m: MeaningfulSeries[Int, Rational] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            if (m.series.values.isEmpty) {
              Prop(m.series.reduce(Mean[Rational]) == NA)
            } else {
              val mean = m.series.values.qsum / m.series.values.length
              Prop(m.series.reduce(Mean[Rational]) == Value(mean))
            }
          }
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Mean[Double])
  }

  "Median" should {
    "find median in dense series" in {
      unique.dense.reduce(Median[Double]) should === (Value(3D))
      duplicate.dense.reduce(Median[Double]) should === (Value(3.5D))
      odd.dense.reduce(Median[Double]) should === (Value(2D))
    }

    "find median in sparse series" in {
      odd.sparse.reduce(Median[Double]) should === (Value(4D))
    }

    "find median in dense series by key" in {
      duplicate.dense.reduceByKey(Median[Double]) should === (Series("a" -> 1.5D, "b" -> 4D, "c" -> 6D))
    }

    "find median in sparse series by key" in {
      duplicate.sparse.reduceByKey(Median[Double]) should === (
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(3D), "d" -> Value(0D)))
    }

    "return the median value of a series" in {
      check { m: MeaningfulSeries[Int, Double] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            val dense = m.series.values.sorted

            if (dense.isEmpty) {
              Prop(m.series.reduce(Median[Double]) == NA)
            } else {
              val dense = m.series.values.sorted
              val l = (dense.size - 1) / 2
              val u = dense.size / 2
              val median = (dense(l) + dense(u)) / 2

              Prop(m.series.reduce(Median[Double]) == Value(median))
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
      unique.dense.reduce(Sum[Double]) should === (Value(12D))
      duplicate.dense.reduce(Sum[Double]) should === (Value(21D))
    }

    "sum sparse series with additive monoid" in {
      odd.sparse.reduce(Sum[Double]) should === (Value(11D))
    }

    "sum dense series by key with additive monoid" in {
      duplicate.dense.reduceByKey(Sum[Double]) shouldBe Series("a" -> 3D, "b" -> 12D, "c" -> 6D)
    }

    "sum sparse series by key with additive monoid" in {
      duplicate.sparse.reduceByKey(Sum[Double]) shouldBe Series("a" -> 0D, "b" -> NM, "c" -> 6D, "d" -> 0D)
    }

    "return the monoidal reduction for a series" in {
      check { ms: MeaningfulSeries[Int, Int] =>
        classifyEmpty(ms.series) {
          classifySparse(ms.series) {
            if (ms.series.values.isEmpty) {
              // For empty m.series, ensure the reducers return the identity value
              {
                implicit val m = Monoid.additive[Int]
                Prop(ms.series.reduce(MonoidReducer[Int]) == Value(m.id))
              } && {
                implicit val m = Monoid.multiplicative[Int]
                Prop(ms.series.reduce(MonoidReducer[Int]) == Value(m.id))
              }
            } else {
              // For non-empty m.series, ensure the reducers return the correct value
              {
                implicit val m = Monoid.additive[Int]
                Prop(ms.series.reduce(MonoidReducer[Int]) == Value(ms.series.values.sum))
              } && {
                implicit val m = Monoid.multiplicative[Int]
                Prop(ms.series.reduce(MonoidReducer[Int]) == Value(ms.series.values.product))
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

      s.reduceByKey(SemigroupReducer[String]) should === (
        Series.fromCells(1 -> Value("ab"), 2 -> Value("c"), 3 -> Value("dee"), 4 -> NA))
    }

    "return the semi-group reduction of a series" in {
      check { m: MeaningfulSeries[Int, Int] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            if (m.series.values.isEmpty) {
              // For empty m.series, ensure the reducers return NA
              {
                implicit val sg = Semigroup.additive[Int]
                Prop(m.series.reduce(SemigroupReducer[Int]) == NA)
              } && {
                implicit val sg = Semigroup.multiplicative[Int]
                Prop(m.series.reduce(SemigroupReducer[Int]) == NA)
              }
            } else {
              // For non-empty m.series, ensure the reducers return the correct value
              {
                implicit val sg = Semigroup.additive[Int]
                Prop(m.series.reduce(SemigroupReducer[Int]) == Value(m.series.values.sum))
              } && {
                implicit val sg = Semigroup.multiplicative[Int]
                Prop(m.series.reduce(SemigroupReducer[Int]) == Value(m.series.values.product))
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
      unique   .dense.reduce(Unique[Double]) should === (Value(Set(1D, 2D, 4D, 5D)))
      odd      .dense.reduce(Unique[Double]) should === (Value(Set(1D, 2D, 3D)))
      duplicate.dense.reduce(Unique[Double]) should === (Value(Set(1D, 2D, 3D, 4D, 5D, 6D)))

      Series(1 -> 1, 2 -> 1, 3 -> 2, 4 -> 1, 5 -> 3, 6 -> 2).reduce(Unique[Int]) should === (Value(Set(1, 2, 3)))
    }

    "return unique elements in sparse series" in {
      odd.sparse.reduce(Unique[Double]) should === (Value(Set(2D, 4D, 5D)))

      val s = Series.fromCells(1 -> Value("a"), 2 -> NA, 1 -> Value("b"), 3 -> NA)
      s.reduce(Unique[String]) should === (Value(Set("a", "b")))
    }

    "return unique elements from dense series by key" in {
      duplicate.dense.reduceByKey(Unique[Double]) should === (
        Series("a" -> Set(1D, 2D), "b" -> Set(3D, 4D, 5D), "c" -> Set(6D)))
    }

    "return unique elements from sparse series by key" in {
      duplicate.sparse.reduceByKey(Unique[Double]) shouldBe
        Series.fromCells("a" -> Value(Set.empty), "b" -> NM, "c" -> Value(Set(1D, 5D)), "d" -> Value(Set(0D)))

      val s = Series.fromCells(
        1 -> Value("a"), 1 -> Value("b"),
        2 -> NA, 2 -> Value("c"),
        3 -> Value("d"), 3 -> NA, 3 -> Value("e"), 3 -> NA, 3 -> Value("e"),
        4 -> NA, 4 -> NA)

      s.reduceByKey(Unique[String]) shouldBe Series(1 -> Set("a", "b"), 2 -> Set("c"), 3 -> Set("d", "e"), 4 -> Set.empty)
    }

    "return the unique values for a series" in {
      check { m: MeaningfulSeries[Int, Int] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            if (m.series.values.isEmpty) {
              Prop(m.series.reduce(Unique[Int]) == Value(Set.empty))
            } else {
              Prop(m.series.reduce(Unique[Int]) == Value(m.series.values.toSet))
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
      unique.dense.reduce(Exists[Double](_ => true))  should === (Value(true))
      unique.dense.reduce(Exists[Double](_ => false)) should === (Value(false))
      unique.dense.reduce(Exists[Double](d => d < 2D)) should === (Value(true))
    }

    "existentially quantify predicate over sparse series" in {
      unique   .sparse.reduce(Exists[Double](d => d < 3D)) should === (Value(true))
      odd      .sparse.reduce(Exists[Double](d => d < 3D)) should === (Value(true))
      duplicate.sparse.reduce(Exists[Double](d => d < 3D)) should === (Value(true))
    }

    "existentially quantify predicate over a dense series by key" in {
      duplicate.dense.reduceByKey(Exists[Double](d => d < 2D)) should === (
        Series("a" -> true, "b" -> false, "c" -> false))
    }

    "existentially quantify predicate over sparse series by key" in {
      duplicate.sparse.reduceByKey(Exists[Double](d => d < 2D)) should === (
        Series("a" -> false, "b" -> false, "c" -> true, "d" -> true))
    }

    "return whether or not a predicate exists for a series" in {
      check { m: MeaningfulSeries[Int, Int] =>
        classifyEmpty(m.series) {
          classifySparse(m.series) {
            if (m.series.values.isEmpty) {
              Prop(m.series.reduce(Exists[Int](pAll)) == Value(false))
            } else {
              classify(m.series.values.exists(pMod10), "exists=true", "exists=false") {
                Prop(m.series.reduce(Exists(pNone)) == Value(false)) &&
                Prop(m.series.reduce(Exists(pMod10)) == Value(m.series.values.exists(pMod10)))
              }
            }
          }
        }
      }
    }
  }

  "Quantile" should {
    "return NA for empty series" in {
      check((es: EmptySeries[Int, Rational]) =>
        collect(es.series.size) {
          Prop(es.series.reduce(Quantile[Rational](List(0.25, 0.5, 0.75))) == NA)
        },
      minSuccessful(10))
    }

    "return min value for 0p" in {
      check {
        forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
          classifySparse(series) {
            val min = series.values.min

            Prop(series.reduce(Quantile[Rational](List(0.0))) == Value(List(0.0 -> min)))
          }
        }
      }
    }

    "return max value for 1p" in {
      check {
        forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
          classifySparse(series) {
            val max = series.values.max
            series.reduce(Quantile[Rational](List(1.0))) == Value(List(1.0 -> max))
          }
        }
      }
    }

    "never return percentile below min or above max" in {
      check {
        forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
          forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
            classifySparse(series) {
              val min = series.values.min
              val max = series.values.max

              Prop(series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (_, q) =>
                q >= min && q <= max
              })
            }
          }
        }
      }
    }

    "percentiles split at appropriate mark" in {
      check {
        forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
          forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
            Prop(series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (p, q) =>
              val below = math.ceil(series.values.size * p)
              val above = math.ceil(series.values.size * (1 - p))
              series.values.count(_ < q) <= below && series.values.count(_ > q) <= above
            })
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
      unique.dense.reduce(ForAll[Double](_ => true))   should === (Value(true))
      unique.dense.reduce(ForAll[Double](_ => false))  should === (Value(false))
      unique.dense.reduce(ForAll[Double](d => d > 0D)) should === (Value(true))
    }

    "universally quantify predicate over a sparse series with only unavailablity" in {
      odd.sparse.reduce(ForAll[Double](d => d > 0D)) should === (Value(true))
    }

    "universally quantify predicate over a sparse series with not meaningful values" in {
      unique   .sparse.reduce(ForAll[Double](_ => true))  should === (Value(false))
      unique   .sparse.reduce(ForAll[Double](_ => false)) should === (Value(false))
      duplicate.sparse.reduce(ForAll[Double](_ => true))  should === (Value(false))
      duplicate.sparse.reduce(ForAll[Double](_ => false)) should === (Value(false))
    }

    "universally quantify predicate over a dense series by key" in {
      duplicate.dense.reduceByKey(ForAll[Double](_ => false)) should === (
        Series("a" -> false, "b" -> false, "c" -> false))
      duplicate.dense.reduceByKey(ForAll[Double](_ => true)) should === (
        Series("a" -> true, "b" -> true, "c" -> true))
      duplicate.dense.reduceByKey(ForAll[Double](d => d < 6D)) should === (
        Series("a" -> true, "b" -> true, "c" -> false))
    }

    "universally quantify predicate over a sparse series by key" in {
      duplicate.sparse.reduceByKey(ForAll[Double](_ => false)) should === (
        Series("a" -> true, "b" -> false, "c" -> false, "d" -> false))
      duplicate.sparse.reduceByKey(ForAll[Double](_ => true)) should === (
        Series("a" -> true, "b" -> false, "c" -> true, "d" -> true))
      duplicate.sparse.reduceByKey(ForAll[Double](d => d < 5D)) should === (
        Series("a" -> true, "b" -> false, "c" -> false, "d" -> true))
    }

    "return true for an empty series" in {
      check((es: EmptySeries[Int, Int]) =>
        collect(es.series.size) {
          Prop(es.series.reduce(ForAll[Int](pFalse)) == Value(true))
        },
        minSuccessful(10))
    }

    "return false for a series that contains NM" in {
      check {
        forAll(arbitrary[Series[Int, Int]]) { series =>
          classifyMeaningful(series) {
            Prop(series.reduce(ForAll[Int](pTrue)) == Value(!series.cells.contains(NM)))
          }
        }
      }
    }

    "evaluate the predicate for a series" in {
      check {
        forAll(arbitrary[MeaningfulSeries[Int, Int]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
          classifySparse(series) {
            classify(series.values.min > 0, "forall=true", "forall=false") {
              Prop(series.reduce(ForAll[Int](pPositive)) == Value(series.values.min > 0))
            }
          }
        }
      }
    }
  }
}
