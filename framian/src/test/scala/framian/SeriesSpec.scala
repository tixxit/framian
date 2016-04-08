package framian

import org.scalacheck._

import org.scalatest.prop.{ Checkers, PropertyChecks }

import scala.collection.mutable
import scala.reflect.ClassTag

import spire.algebra._
import spire.math.Rational
import spire.std.double._
import spire.std.int._
import spire.std.iterable._
import spire.std.string._
import spire.std.tuples._

class SeriesSpec extends FramianSpec
  with PropertyChecks
  with Checkers
  with SeriesClassifiers {

  import Arbitrary.arbitrary
  import Prop._
  import SeriesGenerators._

  implicit val arbRational = Arbitrary(arbitrary[Double].map(Rational(_)))
  implicit def rationalMetricSpace = new MetricSpace[Rational, Double] {
    def distance(v: Rational, w: Rational) = (v - w).abs.toDouble
  }

  "equals" should {
    "have a sane equality" in {
      Series("a" -> 0, "b" -> 1, "c" -> 2) should !== (Series("b" -> 1, "a" -> 0, "c" -> 2))
      Series("a" -> 7) should === (Series(Index.fromKeys("a"), Column.dense(Array(7))))
      Series("a" -> 7) should === (Series(Index("a" -> 0), Column.eval(_ => Value(7))))
      Series("a" -> 7) should === (Series(Index("a" -> 42), Column.eval(_ => Value(7))))
      Series.empty[String, String] should === (Series.empty[String, String])
    }
  }

  "hasValues" should {
    "detect values" in {
      forAll(arbitrary[Series[String, Int]]) { series =>
        series.hasValues should === (series.values.nonEmpty)
        series.filterByCells(_.isNonValue).hasValues should === (false)
      }
    }
  }

  "isEmpty" should {
    "detect an absence of values" in {
      forAll(arbitrary[Series[String, Int]]) { series =>
        series.isEmpty should !== (series.values.nonEmpty)
        series.filterByCells(_.isNonValue).isEmpty should === (true)
      }
    }
  }

  "++" should {
    "concatenate on series after the other" in {
      val s0 = Series.fromCells(1 -> Value("a"), 1 -> Value("b"), 2 -> Value("e"))
      val s1 = Series.fromCells(0 -> Value("c"), 1 -> Value("d"))
      (s0 ++ s1) should === (Series.fromCells(
          1 -> Value("a"), 1 -> Value("b"), 2 -> Value("e"), 0 -> Value("c"), 1 -> Value("d")))
    }

    "respect original traversal order" in {
      val s0 = Series.fromCells(2 -> Value("a"), 1 -> Value("b"))
      val s1 = Series.fromCells(1 -> Value("c"), 0 -> Value("d"))
      (s0 ++ s1) should === (Series.fromCells(
          2 -> Value("a"), 1 -> Value("b"), 1 -> Value("c"), 0 -> Value("d")))
    }
  }

  "orElse" should {
    "merge series with different number of rows" in {
      val s0 = Series.fromCells(1 -> Value("a"), 1 -> Value("b"), 2 -> Value("e"))
      val s1 = Series.fromCells(0 -> Value("c"), 1 -> Value("d"))
      (s0 orElse s1) should === (Series.fromCells(0 -> Value("c"), 1 -> Value("a"), 1 -> Value("b"), 2 -> Value("e")))
    }

    "always let Value take precedence over all" in {
      val seriesA = Series.fromCells("a" -> Value("a"), "b" -> NA, "c" -> NM)
      val seriesB = Series.fromCells("a" -> NM, "b" -> Value("b"), "c" -> NA)
      val seriesC = Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value("c"))

      val mergedSeries = seriesA orElse seriesB orElse seriesC
      mergedSeries("a") should === (Value("a"))
      mergedSeries("b") should === (Value("b"))
      mergedSeries("c") should === (Value("c"))
    }

    "merge series from left to right and maintain all keys" in {
      val seriesOne = Series.fromCells("a" -> Value("a1"), "b" -> Value("b1"), "c" -> NM)
      val seriesOther = Series.fromCells("b" -> Value("b2"), "c" -> Value("c2"), "x" -> Value("x2"), "y" -> NA)
      val mergedSeriesOne = seriesOne orElse seriesOther
      val mergedSeriesOther = seriesOther orElse seriesOne

      // Ensure all keys are available, grouping by (one |+| other)
      mergedSeriesOne("a") should === (Value("a1"))
      mergedSeriesOne("b") should === (Value("b1"))
      mergedSeriesOne("c") should === (Value("c2"))
      mergedSeriesOne("x") should === (Value("x2"))
      mergedSeriesOne("y") should === (NA)

      // Ensure all keys are available, grouping by (other |+| one)
      mergedSeriesOther("a") should === (Value("a1"))
      mergedSeriesOther("b") should === (Value("b2"))
      mergedSeriesOther("c") should === (Value("c2"))
      mergedSeriesOther("x") should === (Value("x2"))
      mergedSeriesOther("y") should === (NA)
    }
  }

  "merge" should {
    "merge series with different number of rows" in {
      val s0 = Series.fromCells(1 -> Value("a"), 1 -> Value("b"), 2 -> Value("e"))
      val s1 = Series.fromCells(0 -> Value("c"), 1 -> Value("d"))
      (s0 merge s1) should === (Series.fromCells(0 -> Value("c"), 1 -> Value("ad"), 1 -> Value("b"), 2 -> Value("e")))
    }

    "use Semigroup.op to cogroup values together in merge" in {
      // Contrive a semigroup by adding all the numbers together of the group
      implicit val intSemigroup = Semigroup.additive[Int]

      forAll(SeriesGenerators.genSeries(arbitrary[Int], arbitrary[Int], (1, 0, 0))) { series =>
        val mergedSeries1 = series.merge(series)
        series.size should === (mergedSeries1.size)
        series.keys.foreach { key =>
          mergedSeries1(key).get should === (series(key).get * 2)
        }

        // Merge once more and ensure it becomes triple the original value
        val mergedSeries2 = mergedSeries1 merge series
        series.size should === (mergedSeries2.size)
        series.keys.foreach { key =>
          mergedSeries2(key).get should === (series(key).get * 3)
        }
      }
    }

    "always let NM take precedence over all" in {
      val seriesA = Series.fromCells("a" -> Value("a"), "b" -> NA, "c" -> NM)
      val seriesB = Series.fromCells("a" -> NM, "b" -> Value("b"), "c" -> NA)
      val seriesC = Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value("c"))

      val mergedSeries = seriesA.merge(seriesB).merge(seriesC)
      mergedSeries("a") should === (NM)
      mergedSeries("b") should === (NM)
      mergedSeries("c") should === (NM)
    }

    "always let Value take precedence over NA" in {
      val seriesA = Series.fromCells("a" -> Value("a"), "b" -> NA, "c" -> NA)
      val seriesB = Series.fromCells("a" -> NA, "b" -> Value("b"), "c" -> NA)
      val seriesC = Series.fromCells("a" -> NA, "b" -> NA, "c" -> Value("c"))

      // Ensure the Value cells simply clobber the NA cells
      val mergedSeries = seriesA.merge(seriesB).merge(seriesC)
      mergedSeries("a") should === (Value("a"))
      mergedSeries("b") should === (Value("b"))
      mergedSeries("c") should === (Value("c"))

      // Ensure further merges of a previously sparse merge operate normally
      val doubleMergedSeries = mergedSeries.merge(seriesC).merge(seriesB).merge(seriesA)
      doubleMergedSeries("a") should === (Value("aa"))
      doubleMergedSeries("b") should === (Value("bb"))
      doubleMergedSeries("c") should === (Value("cc"))
    }

    "merge series from left to right and maintain all keys" in {
      val seriesOne = Series.fromCells("a" -> Value("a1"), "b" -> Value("b1"), "c" -> NM)
      val seriesOther = Series.fromCells("b" -> Value("b2"), "c" -> Value("c2"), "x" -> Value("x2"), "y" -> NA)
      val mergedSeriesOne = seriesOne.merge(seriesOther)
      val mergedSeriesOther = seriesOther.merge(seriesOne)

      // Ensure all keys are available, grouping by (one |+| other)
      mergedSeriesOne("a") should === (Value("a1"))
      mergedSeriesOne("b") should === (Value("b1b2"))
      mergedSeriesOne("c") should === (NM)
      mergedSeriesOne("x") should === (Value("x2"))
      mergedSeriesOne("y") should === (NA)

      // Ensure all keys are available, grouping by (other |+| one)
      mergedSeriesOther("a") should === (Value("a1"))
      mergedSeriesOther("b") should === (Value("b2b1"))
      mergedSeriesOther("c") should === (NM)
      mergedSeriesOther("x") should === (Value("x2"))
      mergedSeriesOther("y") should === (NA)
    }
  }

  "map" should {
    "map values with original order" in {
      val original = Series("a" -> 1, "b" -> 2, "a" -> 3)
      val expected = Series("a" -> 5, "b" -> 6, "a" -> 7)
      original.mapValues(_ + 4) should === (expected)
    }
  }

  "cellMap" should {
    "transform NAs and NMs" in {
      val original = Series.fromCells("a" -> NA, "b" -> NM, "c" -> NA)
      val expected = Series("a" -> 1, "b" -> 2, "c" -> 1)
      original.cellMap {
        case NA => Value(1)
        case NM => Value(2)
        case Value(_) => Value(3)
      } should === (expected)
    }

    "transform Values" in {
      val original = Series("a" -> 1, "b" -> 2, "c" -> 3)
      val expected = Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(5))
      original.cellMap {
        case Value(1) => NA
        case Value(2) => NM
        case Value(n) => Value(n + 2)
        case NA | NM => NA
      } should === (expected)
    }
  }

  "cellMapWithKeys" should {
    "map values with their keys" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              def plus5(c: Cell[Int]): Cell[Int] = c match {
                case Value(v) => Value(v + 5)
                case NA => NA
                case NM => NM
              }

              Prop(series.cellMapWithKeys((k, v) => plus5(v)) == series.cellMap(plus5))
            }
          }
        }
      }
    }
  }

  "zipMap" should {
    "zipMap empty Series" in {
      val empty = Series[String, String]()
      empty.zipMap(empty)(_ + _) should === (empty)
    }

    "zipMap multiple-values-same-key on both sides" in {
      val a = Series("a" -> 1, "a" -> 2)
      val b = Series("a" -> 3, "a" -> 5)
      a.zipMap(b)(_ + _) should === (Series("a" -> 4, "a" -> 6, "a" -> 5, "a" -> 7))
    }

    "zipMap like an inner join" in {
      val a = Series("z" -> 5D, "a" -> 1D, "b" -> 3D, "b" -> 4D)
      val b = Series("a" -> 2, "b" -> 4, "c" -> 3)

      val c = a.zipMap(b) { (x, y) => y * x}
      c should === (Series("a" -> 2D, "b" -> 12D, "b" -> 16D))
    }

    "let NA cells clobber NM and Value cells" in {
      val one = Series.fromCells(   "a" -> Value(1),  "b" -> NA,        "c" -> Value(3),              "d" -> NA,        "d" -> Value(5),  "d" -> NA,        "d" -> Value(7))
      val other = Series.fromCells( "a" -> NA,        "b" -> Value(2),  "c" -> Value(3),  "c" -> NA,  "d" -> Value(5),  "d" -> NM,        "d" -> Value(6),  "d" -> Value(7))

      one.zipMap(other)(_ * _) should === (Series.fromCells(
        "a" -> NA,
        "b" -> NA,
        "c" -> Value(9), "c" -> NA,
        "d" -> NA, "d" -> NA, "d" -> NA, "d" -> NA,
        "d" -> Value(25), "d" -> NM, "d" -> Value(30), "d" -> Value(35),
        "d" -> NA, "d" -> NA, "d" -> NA, "d" -> NA,
        "d" -> Value(35), "d" -> NM, "d" -> Value(42), "d" -> Value(49)
      ))
    }

    "let NM cells clobber NA and Value cells" in {
      val one = Series.fromCells(   "a" -> Value(1),  "b" -> NM,        "c" -> Value(3),              "d" -> NM,        "d" -> Value(5),  "d" -> NA,        "d" -> Value(7))
      val other = Series.fromCells( "a" -> NM,        "b" -> Value(2),  "c" -> Value(3),  "c" -> NM,  "d" -> Value(5),  "d" -> NA,        "d" -> Value(6),  "d" -> NM)

      one.zipMap(other)(_ * _) should === (Series.fromCells(
        "a" -> NM,
        "b" -> NM,
        "c" -> Value(9), "c" -> NM,
        "d" -> NM, "d" -> NA, "d" -> NM, "d" -> NM,
        "d" -> Value(25), "d" -> NA, "d" -> Value(30), "d" -> NM,
        "d" -> NA, "d" -> NA, "d" -> NA, "d" -> NA,
        "d" -> Value(35), "d" -> NA, "d" -> Value(42), "d" -> NM
      ))
    }
  }

  "filterByValues" should {
    "filter a series by its values" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              Prop.all(
                Prop(series.filterByValues(v => true).values == series.values),
                Prop(series.filterByValues(v => false) == Series.empty[String, Int]))
            }
          }
        }
      }
    }
  }

  "filterByKeys" should {
    "filter a series by its keys" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              Prop.all(
                Prop(series.filterByKeys(k => true).values == series.values),
                Prop(series.filterByKeys(k => false) == Series.empty[String, Int]))
            }
          }
        }
      }
    }
  }

  "filterEntries" should {
    "filter a series by its key-value pair entries" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              Prop.all(
                Prop(series.filterEntries((k, v) => true) == series),
                Prop(series.filterEntries((k, v) => false) == Series.empty[String, Int]))
            }
          }
        }
      }
    }
  }

  "foreachDense" should {
    "iterate over all dense key-value pairs" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              // Build a sequence of (K, V) from the dense foreach, and ensure the equivalent of the
              // filterByValues dense series can be recreated from its output
              val eachDenseEntry = Seq.newBuilder[(String, Int)]
              series.foreachDense((k, v) => eachDenseEntry += ((k, v)))

              Prop(series.filterByValues(v => true) == Series(eachDenseEntry.result(): _*))
            }
          }
        }
      }
    }
  }

  "foreachKeys" should {
    "iterate over all keys of a series" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              val keyVectorBuilder = Vector.newBuilder[String]
              series.foreachKeys(keyVectorBuilder += _)

              Prop(series.keys == keyVectorBuilder.result())
            }
          }
        }
      }
    }
  }

  "foreachCells" should {
    "iterate over all values of a series as cells, including NMs and NAs" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              val cellVectorBuilder = Vector.newBuilder[Cell[Int]]
              series.foreachCells(cellVectorBuilder += _)

              Prop(series.cells == cellVectorBuilder.result())
            }
          }
        }
      }
    }
  }

  "foreachValues" should {
    "iterate over all the values of a series" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              val valueVectorBuilder = Vector.newBuilder[Int]
              series.foreachValues(valueVectorBuilder += _)

              Prop(series.values == valueVectorBuilder.result())
            }
          }
        }
      }
    }
  }

  "sorted" should {
    "trivially sort a series" in {
      val a = Series("a" -> 0, "b" -> 1, "c" -> 3).sorted
      a.sorted should === (a)
    }

    "sort an out-of-order series" in {
      Series("c" -> 0, "a" -> 1, "b" -> 3).sorted should === (Series("a" -> 1, "b" -> 3, "c" -> 0))
    }
  }

  "reduce" should {
    "reduce all values" in {
      val a = Series("a" -> 1D, "b" -> 2D, "c" -> 4D, "d" -> 5D)
      val b = Series("c" -> 1D, "a" -> 2D, "b" -> 4D, "a" -> 5D)
      a.reduce(reduce.Mean) should === (Value(3D))
      b.reduce(reduce.Mean) should === (Value(3D))
    }

    "reduce in order" in {
      val a = Series("c" -> 2, "a" -> 1, "b" -> 3)
      a.mapValues(_ :: Nil).reduce[List[Int]](reduce.MonoidReducer) should === (Value(List(2, 1, 3)))
    }
  }

  "reduceByKey" should {
    "trivially reduce groups by key" in {
      val a = Series("a" -> 1D, "a" -> 2D, "a" -> 3D)
      a.reduceByKey(reduce.Count) should === (Series("a" -> 3))
    }

    "reduce groups by key" in {
      val a = Series("c" -> 1D, "a" -> 2D, "b" -> 4D, "a" -> 5D, "b" -> 2D, "b" -> 1D)
      val expected = Series("a" -> (2D + 5D) / 2, "b" -> (1D + 2D + 4D) / 3, "c" -> 1D)
      a.reduceByKey(reduce.Mean) should === (expected)
    }

    "reduce groups by key in order" in {
      val a = Series("c" -> 1, "a" -> 2, "b" -> 3, "a" -> 4, "c" -> 6, "c" -> 5)
      val expected = Series(
        "a" -> List(2, 4),
        "b" -> List(3),
        "c" -> List(1, 6, 5)
      )
      a.mapValues(_ :: Nil).reduceByKey(reduce.MonoidReducer) should === (expected)
    }
  }

  def series[K: Order: ClassTag, V](kvs: (K, Cell[V])*): Series[K, V] = {
    val (keys, cells) = kvs.unzip
    Series(Index.fromKeys(keys: _*), Column(cells: _*))
  }

  "rollForward" should {
    "roll over NAs" in {
      val s = series(1 -> Value("a"), 2 -> NA, 3 -> Value("b"), 4 -> NA, 5 -> NA)
      s.rollForward should === (series(1 -> Value("a"), 2 -> Value("a"), 3 -> Value("b"), 4 -> Value("b"), 5 -> Value("b")))
    }

    "skip initial NAs" in {
      val s = series(1 -> NA, 2 -> NA, 3 -> Value("b"), 4 -> NA, 5 -> NA)
      s.rollForward should === (series(1 -> NA, 2 -> NA, 3 -> Value("b"), 4 -> Value("b"), 5 -> Value("b")))
    }

    "roll NMs forward" in {
      val s0 = series(1 -> NA, 2 -> NM, 3 -> NA)
      val s1 = series(1 -> Value("a"), 2 -> NM, 3 -> NA, 4 -> Value("b"))

      s0.rollForward should === (series(1 -> NA, 2 -> NM, 3 -> NM))
      s1.rollForward should === (series(1 -> Value("a"), 2 -> NM, 3 -> NM, 4 -> Value("b")))
    }
  }

  "rollForwardUpTo" should {
    "only roll up to limit" in {
      val s0 = series(1 -> Value("a"), 2 -> NA, 3 -> NA, 4 -> NA)
      val s1 = series(0 -> NA, 1 -> Value("a"), 2 -> NA, 3 -> NA, 4 -> NM, 5 -> Value("b"))

      s0.rollForwardUpTo(1) should === (series(1 -> Value("a"), 2 -> Value("a"), 3 -> NA, 4 -> NA))
      s0.rollForwardUpTo(2) should === (series(1 -> Value("a"), 2 -> Value("a"), 3 -> Value("a"), 4 -> NA))
      s1.rollForwardUpTo(1) should === (series(0 -> NA, 1 -> Value("a"), 2 -> Value("a"), 3 -> NA, 4 -> NM, 5 -> Value("b")))
    }

    "roll NMs" in {
      val s0 = series(1 -> NM, 2 -> NA, 3 -> NA, 4 -> NA)
      s0.rollForwardUpTo(1) should === (series(1 -> NM, 2 -> NM, 3 -> NA, 4 -> NA))
      s0.rollForwardUpTo(2) should === (series(1 -> NM, 2 -> NM, 3 -> NM, 4 -> NA))
    }
  }

  "find(First|Last)Value" should {
    "get first/last value in series" in {
      val s0 = Series(1 -> "x", 2 -> "y")
      s0.findFirstValue should === (Some(1 -> "x"))
      s0.findLastValue should === (Some(2 -> "y"))

      val s1 = Series.fromCells(1 -> NA, 2 -> NM, 3 -> Value("a"), 4 -> NA, 5 -> Value("b"), 6 -> NA)
      s1.findFirstValue should === (Some(3 -> "a"))
      s1.findLastValue should === (Some(5 -> "b"))
    }
  }

  "closestKeyTo" should {
    "always return None for a series with no keys" in {
      val s = Series[Double, Double]()
      forAll (arbitrary[(Double, Double)]) { case (source, tolerance) =>
        s.closestKeyTo(source, tolerance) should === (None)
      }
    }

    "return the value that is closest to the given key" in {
      val s = Series.fromCells[Double, Double](1d -> NM, 2d -> NA, 3d -> NA, 4d -> NM, 5d -> NM)
      s.closestKeyTo(6, 1) should === (Some(5d))
      s.closestKeyTo(6, 0.9) should === (None)
      s.closestKeyTo(4.5, 0.5) should === (Some(4d))
      s.closestKeyTo(3.5, 0.4) should === (None)
    }

    "always return a value within the tolerance, or None" in {
      check { (series: Series[Rational, Rational], source: Rational, toleranceNegative: Double) =>
        val tolerance = Math.abs(toleranceNegative)
        series.closestKeyTo(source, tolerance) match {
          case Some(value) => {
            // Ensure the closest key is within the tolerance
            collect("hit") {
              Prop((source - value).abs.toDouble <= tolerance)
            }
          }
          case None => {
            // There was no closest, ensure all keys are outside the tolerance
            collect("miss") {
              Prop(series.keys.forall { key =>
                (key - source).abs.toDouble > tolerance
              })
            }
          }
        }
      }
    }
  }

  "count" should {
    "render the number of items in the series" in {
      check { (series: Series[String, Int]) =>
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              if (series.filterByCells(_ == NM).cells.nonEmpty) {
                Prop(series.count == NM)
              } else {
                Prop(series.count == Value(series.values.size))
              }
            }
          }
        }
      }
    }
  }

  "first" should {
    "render the first valid value in a series" in {
      Series[String, Int]().first should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).first should === (Value(1))
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).first should === (Value(2))
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> Value(3)).first should === (Value(3))
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> NA).first should be (NA)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> NM).first should === (Value(1))
      Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(3)).first should === (NM)
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> NM).first should be (NM)
    }
  }

  "firstN" should {
    "render the first N valid values in a series" in {
      Series[String, Int]().first should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).firstN(2) should === (Value(List(1, 2)))
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).firstN(2) should === (Value(List(2, 3)))
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> Value(3)).firstN(2) should === (NA)
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> NA).firstN(2) should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NM).firstN(2) should === (Value(List(1, 2)))
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).firstN(2) should === (NM)
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> NM).firstN(2) should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> Value(3)).firstN(2) should === (Value(List(1, 3)))
    }
  }

  "last" should {
    "render the last valid value in a series" in {
      Series[String, Int]().first should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).last should === (Value(3))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).last should === (Value(2))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).last should === (Value(1))
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> NA).last should be (NA)
      Series.fromCells("a" -> NM, "b" -> NM, "c" -> Value(3)).last should === (Value(3))
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> NA).last should === (NM)
      Series.fromCells("a" -> NM, "b" -> NA, "c" -> NA).last should be (NM)
    }
  }

  "lastN" should {
    "render the last N valid values in a series" in {
      Series[String, Int]().lastN(2) should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).lastN(2) should === (Value(List(2, 3)))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).lastN(2) should === (Value(List(1, 2)))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).lastN(2) should === (NA)
      Series.fromCells("a" -> NA, "b" -> NA, "c" -> NA).lastN(2) should === (NA)
      Series.fromCells("a" -> NM, "b" -> Value(2), "c" -> Value(3)).lastN(2) should === (Value(List(2, 3)))
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).lastN(2) should === (NM)
      Series.fromCells("a" -> NM, "b" -> NA, "c" -> NA).lastN(2) should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> Value(3)).lastN(2) should === (Value(List(1, 3)))
    }
  }

  "max" should {
    "render the maximum value in a series" in {
      Series[String, Int]().max should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).max should === (Value(3))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).max should === (Value(2))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).max should === (Value(1))
      Series.fromCells[String, Int]("a" -> NA, "b" -> NA, "c" -> NA).max should === (NA)
      Series.fromCells[String, Int]("a" -> NM, "b" -> Value(2), "c" -> Value(3)).max should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).max should === (NM)
      Series.fromCells[String, Int]("a" -> NM, "b" -> NA, "c" -> NA).max should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> Value(3)).max should === (Value(3))
    }
  }

  "min" should {
    "render the minimum value in a series" in {
      Series[String, Int]().min should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).min should === (Value(1))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).min should === (Value(1))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).min should === (Value(1))
      Series.fromCells[String, Int]("a" -> NA, "b" -> NA, "c" -> NA).min should === (NA)
      Series.fromCells[String, Int]("a" -> NM, "b" -> Value(2), "c" -> Value(3)).min should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).min should === (NM)
      Series.fromCells[String, Int]("a" -> NM, "b" -> NA, "c" -> NA).min should === (NM)
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).min should === (Value(2))
    }
  }

  "sum" should {
    "render a sum of all values in a series" in {
      Series[String, Int]().sum should === (Value(0))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).sum should === (Value(6))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).sum should === (Value(3))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).sum should === (Value(1))
      Series.fromCells[String, Int]("a" -> NA, "b" -> NA, "c" -> NA).sum should === (Value(0))
      Series.fromCells[String, Int]("a" -> NM, "b" -> Value(2), "c" -> Value(3)).sum should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).sum should === (NM)
      Series.fromCells[String, Int]("a" -> NM, "b" -> NA, "c" -> NA).sum should === (NM)
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).sum should === (Value(5))
    }
  }

  "sumNonEmpty" should {
    "render a sum of all the values in non-empty series" in {
      Series[String, Int]().sumNonEmpty should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).sumNonEmpty should === (Value(6))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).sumNonEmpty should === (Value(3))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).sumNonEmpty should === (Value(1))
      Series.fromCells[String, Int]("a" -> NA, "b" -> NA, "c" -> NA).sumNonEmpty should === (NA)
      Series.fromCells[String, Int]("a" -> NM, "b" -> Value(2), "c" -> Value(3)).sumNonEmpty should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).sumNonEmpty should === (NM)
      Series.fromCells[String, Int]("a" -> NM, "b" -> NA, "c" -> NA).sumNonEmpty should === (NM)
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).sumNonEmpty should === (Value(5))
    }
  }

  "product" should {
    "render a product of all values in a series" in {
      Series[String, Int]().product should === (Value(1))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).product should === (Value(6))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).product should === (Value(2))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).product should === (Value(1))
      Series.fromCells[String, Int]("a" -> NA, "b" -> NA, "c" -> NA).product should === (Value(1))
      Series.fromCells[String, Int]("a" -> NM, "b" -> Value(2), "c" -> Value(3)).product should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).product should === (NM)
      Series.fromCells[String, Int]("a" -> NM, "b" -> NA, "c" -> NA).product should === (NM)
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).product should === (Value(6))
    }
  }

  "productNonEmpty" should {
    "render a product of all the values in non-empty series" in {
      Series[String, Int]().productNonEmpty should === (NA)
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> Value(3)).productNonEmpty should === (Value(6))
      Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA).productNonEmpty should === (Value(2))
      Series.fromCells("a" -> Value(1), "b" -> NA, "c" -> NA).productNonEmpty should === (Value(1))
      Series.fromCells[String, Int]("a" -> NA, "b" -> NA, "c" -> NA).productNonEmpty should === (NA)
      Series.fromCells[String, Int]("a" -> NM, "b" -> Value(2), "c" -> Value(3)).productNonEmpty should === (NM)
      Series.fromCells("a" -> Value(1), "b" -> NM, "c" -> Value(3)).productNonEmpty should === (NM)
      Series.fromCells[String, Int]("a" -> NM, "b" -> NA, "c" -> NA).productNonEmpty should === (NM)
      Series.fromCells("a" -> NA, "b" -> Value(2), "c" -> Value(3)).productNonEmpty should === (Value(6))
    }
  }

  "histogram" should {
    "return 0 counts on empty input series" in {
      Series[String, Int]().histogram(0, 10, 2) should === (
        Series((0, 2) -> 0, (2, 4) -> 0, (4, 6) -> 0, (6, 8) -> 0, (8, 10) -> 0))
    }

    "exclude values over max" in {
      Series(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5).histogram(0, 1, 2) should === (
        Series((0, 1) -> 1))

      Series(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6).histogram(0, 5, 2) should === (
        Series((0, 2) -> 1, (2, 4) -> 2, (4, 5) -> 2))
    }

    "max is inclusive" in {
      Series("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5).histogram(0, 4, 2) should === (
        Series((0, 2) -> 1, (2, 4) -> 3))
    }

    "exclude values less than min" in {
      Series("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5).histogram(2, 6, 2) should === (
        Series((2, 4) -> 2, (4, 6) -> 2))

      Series("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5).histogram(3, 6, 2) should === (
        Series((3, 5) -> 2, (5, 6) -> 1))
    }
  }

  "normalizedHistogram" should {
    "return percentage of cells in bucket" in {
      Series((1 to 25).zipWithIndex: _*).normalizedHistogram[Double](0, 24, 5) should === (
        Series((0, 5) -> 0.2, (5, 10) -> 0.2, (10, 15) -> 0.2, (15, 20) -> 0.2, (20, 24) -> 0.2))
    }

    "include NAs and NMs in total size" in {
      Series.fromCells("a" -> Value(0), "b" -> NA, "c" -> Value(1), "D" -> Value(2)).normalizedHistogram[Double](0, 4, 2) should === (
        Series((0, 2) -> 0.5, (2, 4) -> 0.25))

      Series.fromCells("a" -> Value(0), "b" -> Value(3), "c" -> NM, "D" -> Value(2)).normalizedHistogram[Double](0, 4, 2) should === (
        Series((0, 2) -> 0.25, (2, 4) -> 0.5))

      Series.fromCells("a" -> Value(0), "b" -> NA, "c" -> NM, "D" -> Value(2)).normalizedHistogram[Double](0, 4, 2) should === (
        Series((0, 2) -> 0.25, (2, 4) -> 0.25))
    }
  }
}
