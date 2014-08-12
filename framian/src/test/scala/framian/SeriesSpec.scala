package framian

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._

import scala.reflect.ClassTag

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._
import spire.std.iterable._

class SeriesSpec extends Specification with ScalaCheck {
  import Arbitrary.arbitrary
  import Prop.forAll
  import SeriesGenerators._

  "Series" should {
    "have a sane equality" in {
      Series("a" -> 0, "b" -> 1, "c" -> 2) must_!= Series("b" -> 1, "a" -> 0, "c" -> 2)
      Series("a" -> 7) must_== Series(Index.fromKeys("a"), Column.fromArray(Array(7)))
      Series("a" -> 7) must_== Series(Index("a" -> 0), Column(_ => 7))
      Series("a" -> 7) must_== Series(Index("a" -> 42), Column(_ => 7))
      Series.empty[String, String] must_== Series.empty[String, String]
    }

    "detect values using hasValues" in {
      forAll(arbitrary[Series[String, Int]]) { series =>
        series.hasValues must_== series.values.nonEmpty
        series.filterValues(_.isNonValue).hasValues must_== false
      }
    }

    "map values with original order" in {
      val original = Series("a" -> 1, "b" -> 2, "a" -> 3)
      val expected = Series("a" -> 5, "b" -> 6, "a" -> 7)
      original.mapValues(_ + 4) must_== expected
    }

    "zipMap empty Series" in {
      val empty = Series[String, String]()
      empty.zipMap(empty)(_ + _) must_== empty
    }

    "zipMap multiple-values-same-key on both sides" in {
      val a = Series("a" -> 1, "a" -> 2)
      val b = Series("a" -> 3, "a" -> 5)
      a.zipMap(b)(_ + _) must_== Series("a" -> 4, "a" -> 6, "a" -> 5, "a" -> 7)
    }

    "zipMap like an inner join" in {
      val a = Series("z" -> 5D, "a" -> 1D, "b" -> 3D, "b" -> 4D)
      val b = Series("a" -> 2, "b" -> 4, "c" -> 3)
      
      val c = a.zipMap(b) { (x, y) => y * x }
      c must_== Series("a" -> 2D, "b" -> 12D, "b" -> 16D)
    }

    "trivially sort a series" in {
      val a = Series("a" -> 0, "b" -> 1, "c" -> 3).sorted
      a.sorted must_== a
    }

    "sort an out-of-order series" in {
      Series("c" -> 0, "a" -> 1, "b" -> 3).sorted must_== Series("a" -> 1, "b" -> 3, "c" -> 0)
    }

    "reduce all values" in {
      val a = Series("a" -> 1D, "b" -> 2D, "c" -> 4D, "d" -> 5D)
      val b = Series("c" -> 1D, "a" -> 2D, "b" -> 4D, "a" -> 5D)
      a.reduce(reduce.Mean) must_== Value(3D)
      b.reduce(reduce.Mean) must_== Value(3D)
    }

    "reduce in order" in {
      val a = Series("c" -> 2, "a" -> 1, "b" -> 3)
      a.mapValues(_ :: Nil).reduce[List[Int]](reduce.MonoidReducer) must_== Value(List(2, 1, 3))
    }

    "trivially reduce groups by key" in {
      val a = Series("a" -> 1D, "a" -> 2D, "a" -> 3D)
      a.reduceByKey(reduce.Count) must_== Series("a" -> 3)
    }
    
    "reduce groups by key" in {
      val a = Series("c" -> 1D, "a" -> 2D, "b" -> 4D, "a" -> 5D, "b" -> 2D, "b" -> 1D)
      val expected = Series("a" -> (2D + 5D) / 2, "b" -> (1D + 2D + 4D) / 3, "c" -> 1D)
      a.reduceByKey(reduce.Mean) must_== expected
    }

    "reduce groups by key in order" in {
      val a = Series("c" -> 1, "a" -> 2, "b" -> 3, "a" -> 4, "c" -> 6, "c" -> 5)
      val expected = Series(
        "a" -> List(2, 4),
        "b" -> List(3),
        "c" -> List(1, 6, 5)
      )
      a.mapValues(_ :: Nil).reduceByKey(reduce.MonoidReducer) must_== expected
    }

    def series[K: Order: ClassTag, V](kvs: (K, Cell[V])*): Series[K, V] = {
      val (keys, cells) = kvs.unzip
      Series(Index.fromKeys(keys: _*), Column.fromCells(cells.toVector))
    }

    "rollForward rolls over NAs" in {
      val s = series(1 -> Value("a"), 2 -> NA, 3 -> Value("b"), 4 -> NA, 5 -> NA)
      s.rollForward must_== series(1 -> Value("a"), 2 -> Value("a"), 3 -> Value("b"), 4 -> Value("b"), 5 -> Value("b"))
    }

    "rollForward skips initial NAs" in {
      val s = series(1 -> NA, 2 -> NA, 3 -> Value("b"), 4 -> NA, 5 -> NA)
      s.rollForward must_== series(1 -> NA, 2 -> NA, 3 -> Value("b"), 4 -> Value("b"), 5 -> Value("b"))
    }

    "rollForward rolls NMs forward" in {
      val s0 = series(1 -> NA, 2 -> NM, 3 -> NA)
      val s1 = series(1 -> Value("a"), 2 -> NM, 3 -> NA, 4 -> Value("b"))

      s0.rollForward must_== series(1 -> NA, 2 -> NM, 3 -> NM)
      s1.rollForward must_== series(1 -> Value("a"), 2 -> NM, 3 -> NM, 4 -> Value("b"))
    }

    "rollForwardUpTo only rolls up to limit" in {
      val s0 = series(1 -> Value("a"), 2 -> NA, 3 -> NA, 4 -> NA)
      val s1 = series(0 -> NA, 1 -> Value("a"), 2 -> NA, 3 -> NA, 4 -> NM, 5 -> Value("b"))

      s0.rollForwardUpTo(1) must_== series(1 -> Value("a"), 2 -> Value("a"), 3 -> NA, 4 -> NA)
      s0.rollForwardUpTo(2) must_== series(1 -> Value("a"), 2 -> Value("a"), 3 -> Value("a"), 4 -> NA)
      s1.rollForwardUpTo(1) must_== series(0 -> NA, 1 -> Value("a"), 2 -> Value("a"), 3 -> NA, 4 -> NM, 5 -> Value("b"))
    }

    "rollForwardUpTo rolls NMs" in {
      val s0 = series(1 -> NM, 2 -> NA, 3 -> NA, 4 -> NA)
      s0.rollForwardUpTo(1) must_== series(1 -> NM, 2 -> NM, 3 -> NA, 4 -> NA)
      s0.rollForwardUpTo(2) must_== series(1 -> NM, 2 -> NM, 3 -> NM, 4 -> NA)
    }

    "merge simple series" in {
      val s0 = Series.fromCells(1 -> Value("a"), 2 -> NA,         3 -> NM,         4 -> Value("b"), 5 -> Value("c"), 6 -> NA, 7 -> NA, 8 -> NM)
      val s1 = Series.fromCells(1 -> Value("c"), 2 -> Value("d"), 3 -> Value("e"), 4 -> NA,         5 -> NM,         6 -> NA, 7 -> NM, 8 -> NA)
      s0.merge(s1) must_== Series.fromCells(1 -> Value("ac"), 2 -> Value("d"), 3 -> NM, 4 -> Value("b"), 5 -> NM, 6 -> NA, 7 -> NM, 8 -> NM)
      s1.merge(s0) must_== Series.fromCells(1 -> Value("ca"), 2 -> Value("d"), 3 -> NM, 4 -> Value("b"), 5 -> NM, 6 -> NA, 7 -> NM, 8 -> NM)
    }

    "merge series with different number of rows" in {
      val s0 = Series.fromCells(1 -> Value("a"), 1 -> Value("b"), 2 -> Value("e"))
      val s1 = Series.fromCells(0 -> Value("c"), 1 -> Value("d"))
      (s0 merge s1) must_== Series.fromCells(0 -> Value("c"), 1 -> Value("ad"), 1 -> Value("b"), 2 -> Value("e"))
    }

    "get first/last value in series" in {
      val s0 = Series(1 -> "x", 2 -> "y")
      s0.findFirstValue must_== Some(1 -> "x")
      s0.findLastValue must_== Some(2 -> "y")

      val s1 = Series.fromCells(1 -> NA, 2 -> NM, 3 -> Value("a"), 4 -> NA, 5 -> Value("b"), 6 -> NA)
      s1.findFirstValue must_== Some(3 -> "a")
      s1.findLastValue must_== Some(5 -> "b")
    }
  }
}
