package pellucid.pframe

import org.specs2.mutable._

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._
import spire.std.iterable._

class SeriesSpec extends Specification {
  "Series" should {
    "have a sane equality" in {
      Series("a" -> 0, "b" -> 1, "c" -> 2) must_!= Series("b" -> 1, "a" -> 0, "c" -> 2)
      Series("a" -> 7) must_== Series(Index.fromKeys("a"), Column.fromArray(Array(7)))
      Series("a" -> 7) must_== Series(Index("a" -> 0), Column(_ => 7))
      Series("a" -> 7) must_== Series(Index("a" -> 42), Column(_ => 7))
      Series.empty[String, String] must_== Series.empty[String, String]
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
      a.reduce(reduce.Mean) must_== 3D
      b.reduce(reduce.Mean) must_== 3D
    }

    "reduce in order" in {
      val a = Series("c" -> 2, "a" -> 1, "b" -> 3)
      a.mapValues(_ :: Nil).reduce(reduce.MonoidReducer) must_== List(2, 1, 3)
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
  }
}
