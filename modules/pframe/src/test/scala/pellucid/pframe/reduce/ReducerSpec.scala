package pellucid.pframe
package reduce

import org.specs2.mutable._

import spire.std.any._

class ReducerSpec extends Specification {
  val empty = Series.empty[String, Double]

  object unique {
    val dense = Series("a" -> 1D, "b" -> 2D, "c" -> 4D, "d" -> 5D)
    val sparse = Series(Index.fromKeys("a", "b", "c", "d", "e", "f"),
      Column.fromCells(Vector(NA, Value(2D), NM, NA, Value(4D), NM)))
  }

  object odd {
    val dense = Series("a" -> 1D, "b" -> 2D, "c" -> 3D)
    val sparse = Series(Index.fromKeys("a", "b", "c", "d"),
               Column.fromCells(Vector(NA, Value(2D), Value(4D), Value(5D))))
  }

  object duplicate {
    val dense = Series("a" -> 1D, "a" -> 2D, "b" -> 3D, "b" -> 4D, "b" -> 5D, "c" -> 6D)
    val sparse = Series.fromCells(
      "a" -> NA,
      "b" -> Value(2D), "b" -> NM, "b" -> NA, "b" -> Value(4D), "b" -> NM,
      "c" -> Value(5D), "c" -> NA, "c" -> Value(1D),
      "d" -> Value(0D))
    // val sparse = Series(
    //           Index.fromKeys("a",       "b", "b", "b",       "b", "b",       "c", "c",       "c",       "d"),
    //   Column.fromCells(Vector(NA, Value(2D),  NM,  NA, Value(4D),  NM, Value(5D),  NA, Value(1D), Value(0D))))
  }

  "Mean" should {
    "find mean of empty series" in {
      empty.reduce(Mean[Double]) must_== NM
    }

    "find mean of dense series" in {
      unique.dense.reduce(Mean[Double]) must_== Value(3D)
      duplicate.dense.reduce(Mean[Double]) must_== Value(3.5)
    }

    "find mean of sparse series" in {
      unique.sparse.reduce(Mean[Double]) must_== Value(3D)
      duplicate.sparse.reduce(Mean[Double]) must_== Value(12D / 5D)
    }

    "find mean of dense series by key" in {
      duplicate.dense.reduceByKey(Mean[Double]) must_==
        Series("a" -> 1.5, "b" -> 4D, "c" -> 6D)
    }

    "find mean of sparse series by key" in {
      duplicate.sparse.reduceByKey(Mean[Double]) must_==
        Series.fromCells("a" -> NM, "b" -> Value(3D), "c" -> Value(3D), "d" -> Value(0D))
    }
  }

  "Sum" should {
    "sum empty series" in {
      empty.reduce(Sum[Double]) must_== Value(0D)
    }

    "sum dense series" in {
      unique.dense.reduce(Sum[Double]) must_== Value(12D)
      duplicate.dense.reduce(Sum[Double]) must_== Value(21D)
    }

    "sum sparse series" in {
      unique.sparse.reduce(Sum[Double]) must_== NM
      odd.sparse.reduce(Sum[Double]) must_== Value(11D)
      duplicate.sparse.reduce(Sum[Double]) must_== NM
    }

    "sum dense series by key" in {
      duplicate.dense.reduceByKey(Sum[Double]) must_== Series("a" -> 3D, "b" -> 12D, "c" -> 6D)
    }

    "sum sparse series by key" in {
      duplicate.sparse.reduceByKey(Sum[Double]) must_== Series("a" -> 0D, "b" -> NM, "c" -> 6D, "d" -> 0D)
    }
  }

  "Count" should {
    "count empty series" in {
      empty.reduce(Count) must_== Value(0)
    }

    "count dense series" in {
      unique.dense.reduce(Count) must_== Value(4)
      duplicate.dense.reduce(Count) must_== Value(6)
    }

    "count sparse series" in {
      unique.sparse.reduce(Count) must_== Value(2)
      duplicate.sparse.reduce(Count) must_== Value(5)
    }

    "count dense series by key" in {
      duplicate.dense.reduceByKey(Count) must_== Series("a" -> 2, "b" -> 3, "c" -> 1)
    }

    "count sparse series by key" in {
      duplicate.sparse.reduceByKey(Count) must_== Series("a" -> 0, "b" -> 2, "c" -> 2, "d" -> 1)
    }
  }

  "Max" should {
    "not find max of empty series" in {
      empty.reduce(Max[Double]) must_== NA
    }

    "find max in dense series" in {
      unique.dense.reduce(Max[Double]) must_== Value(5D)
      duplicate.dense.reduce(Max[Double]) must_== Value(6D)
    }

    "find max in sparse series" in {
      unique.sparse.reduce(Max[Double]) must_== Value(4D)
      duplicate.sparse.reduce(Max[Double]) must_== Value(5D)
    }

    "find max in dense series by key" in {
      duplicate.dense.reduceByKey(Max[Double]) must_== Series("a" -> 2D, "b" -> 5D, "c" -> 6D)
    }

    "find max in sparse series by key" in {
      duplicate.sparse.reduceByKey(Max[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> Value(4D), "c" -> Value(5D), "d" -> Value(0D))
    }
  }

  "Median" should {
    "not find median of empty series" in {
      empty.reduce(Median[Double]) must_== NA
    }

    "find median in dense series" in {
      unique.dense.reduce(Median[Double]) must_== Value(3D)
      duplicate.dense.reduce(Median[Double]) must_== Value(3.5D)
      odd.dense.reduce(Median[Double]) must_== Value(2D)
    }

    "find median in sparse series" in {
      unique.sparse.reduce(Median[Double]) must_== Value(3D)
      duplicate.sparse.reduce(Median[Double]) must_== Value(2D)
      odd.sparse.reduce(Median[Double]) must_== Value(4D)
    }

    "find median in dense series by key" in {
      duplicate.dense.reduceByKey(Median[Double]) must_== Series("a" -> 1.5D, "b" -> 4D, "c" -> 6D)
    }

    "find median in sparse series by key" in {
      duplicate.sparse.reduceByKey(Median[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> Value(3D), "c" -> Value(3D), "d" -> Value(0D))
    }
  }

  "First(N)/Last(N)" should {
    "not find the first/last value in empty series" in {
      empty.reduce(First [Double])    must_== NA
      empty.reduce(Last  [Double])    must_== NA
      empty.reduce(FirstN[Double](1)) must_== NA
      empty.reduce(FirstN[Double](2)) must_== NA
      empty.reduce(LastN [Double](1)) must_== NA
      empty.reduce(LastN [Double](2)) must_== NA
    }

    "return NM if the first value encountered is NM" in {
      unique.sparse.reduce(Last [Double])    must_== NM
      unique.sparse.reduce(LastN[Double](1)) must_== NM
      unique.sparse.reduce(LastN[Double](2)) must_== NM
    }

    "get first value in dense series" in {
      unique   .dense.reduce(First[Double]) must_== Value(1D)
      duplicate.dense.reduce(First[Double]) must_== Value(1D)
      odd      .dense.reduce(First[Double]) must_== Value(1D)

      unique   .dense.reduce(FirstN[Double](1)) must_== Value(List(1D))
      duplicate.dense.reduce(FirstN[Double](1)) must_== Value(List(1D))
      odd      .dense.reduce(FirstN[Double](1)) must_== Value(List(1D))
    }

    "get first N values in dense series" in {
      unique   .dense.reduce(FirstN[Double](3)) must_== Value(List(1D, 2D, 4D))
      duplicate.dense.reduce(FirstN[Double](3)) must_== Value(List(1D, 2D, 3D))
      odd      .dense.reduce(FirstN[Double](3)) must_== Value(List(1D, 2D, 3D))
    }

    "get last value in dense series" in {
      unique.dense.reduce(Last[Double]) must_== Value(5D)
      duplicate.dense.reduce(Last[Double]) must_== Value(6D)
      odd.dense.reduce(Last[Double]) must_== Value(3D)

      unique   .dense.reduce(LastN[Double](1)) must_== Value(List(5D))
      duplicate.dense.reduce(LastN[Double](1)) must_== Value(List(6D))
      odd      .dense.reduce(LastN[Double](1)) must_== Value(List(3D))
    }

    "get last N values in dense series" in {
      unique   .dense.reduce(LastN[Double](3)) must_== Value(List(2D, 4D, 5D))
      duplicate.dense.reduce(LastN[Double](3)) must_== Value(List(4D, 5D, 6D))
      odd      .dense.reduce(LastN[Double](3)) must_== Value(List(1D, 2D, 3D))
    }

    "return NA if fewer than N values are available in dense series" in {
      odd.dense.reduce(FirstN[Double](4)) must_== NA
      odd.dense.reduce(LastN[Double](4)) must_== NA
    }

    "get first value of sparse series" in {
      unique.sparse.reduce(First[Double]) must_== Value(2D)
      duplicate.sparse.reduce(First[Double]) must_== Value(2D)
      odd.sparse.reduce(First[Double]) must_== Value(2D)

      unique   .sparse.reduce(FirstN[Double](1)) must_== Value(List(2D))
      duplicate.sparse.reduce(FirstN[Double](1)) must_== Value(List(2D))
      odd      .sparse.reduce(FirstN[Double](1)) must_== Value(List(2D))
    }

    "get first N values in sparse series" in {
      odd.sparse.reduce(FirstN[Double](3)) must_== Value(List(2D, 4D, 5D))
    }

    "get last value of sparse series" in {
      unique.sparse.reduce(Last[Double]) must_== NM
      duplicate.sparse.reduce(Last[Double]) must_== Value(0D)
      odd.sparse.reduce(Last[Double]) must_== Value(5D)

      unique   .sparse.reduce(LastN[Double](1)) must_== NM
      duplicate.sparse.reduce(LastN[Double](1)) must_== Value(List(0D))
      odd      .sparse.reduce(LastN[Double](1)) must_== Value(List(5D))
    }

    "get last N values in sparse series" in {
      odd.sparse.reduce(LastN[Double](3)) must_== Value(List(2D, 4D, 5D))
    }

    "get first in dense series by key" in {
      duplicate.dense.reduceByKey(First[Double]) must_==
        Series("a" -> 1D, "b" -> 3D, "c" -> 6D)

      duplicate.dense.reduceByKey(FirstN[Double](1)) must_==
        Series("a" -> List(1D), "b" -> List(3D), "c" -> List(6D))
    }

    "get first N values in dense series by key" in {
      duplicate.dense.reduceByKey(FirstN[Double](2)) must_==
        Series.fromCells("a" -> Value(List(1D, 2D)), "b" -> Value(List(3D, 4D)), "c" -> NA)

      duplicate.dense.reduceByKey(FirstN[Double](3)) must_==
        Series.fromCells("a" -> NA, "b" -> Value(List(3D, 4D, 5D)), "c" -> NA)
    }

    "get last in dense series by key" in {
      duplicate.dense.reduceByKey(Last[Double]) must_==
        Series("a" -> 2D, "b" -> 5D, "c" -> 6D)

      duplicate.dense.reduceByKey(LastN[Double](1)) must_==
        Series("a" -> List(2D), "b" -> List(5D), "c" -> List(6D))
    }

    "get last N values in dense series by key" in {
      duplicate.dense.reduceByKey(LastN[Double](2)) must_==
        Series.fromCells("a" -> Value(List(1D, 2D)), "b" -> Value(List(4D, 5D)), "c" -> NA)

      duplicate.dense.reduceByKey(LastN[Double](3)) must_==
        Series.fromCells("a" -> NA, "b" -> Value(List(3D, 4D, 5D)), "c" -> NA)
    }

    "get first in sparse series by key" in {
      duplicate.sparse.reduceByKey(First[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> Value(2D), "c" -> Value(5D), "d" -> Value(0D))

      duplicate.sparse.reduceByKey(FirstN[Double](1)) must_==
        Series.fromCells("a" -> NA, "b" -> Value(List(2D)), "c" -> Value(List(5D)), "d" -> Value(List(0D)))
    }

    "get first N values in sparse series by key" in {
      duplicate.sparse.reduceByKey(FirstN[Double](2)) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(5D, 1D)), "d" -> NA)
    }

    "get last in sparse series by key" in {
      duplicate.sparse.reduceByKey(Last[Double]) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(1D), "d" -> Value(0D))

      duplicate.sparse.reduceByKey(LastN[Double](1)) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(1D)), "d" -> Value(List(0D)))
    }

    "get last N values in sparse series by key" in {
      duplicate.sparse.reduceByKey(LastN[Double](2)) must_==
        Series.fromCells("a" -> NA, "b" -> NM, "c" -> Value(List(5D, 1D)), "d" -> NA)
    }
  }

  "Unique" should {
    "return the empty set for an empty series" in {
      empty.reduce(Unique[Double]) must_== Value(Set.empty)
      Series.fromCells[Int, Int](1 -> NA, 2 -> NA).reduce(Unique[Int]) must_== Value(Set.empty)
    }

    "return unique elements from dense series" in {
      Series(1 -> 1, 2 -> 1, 3 -> 2, 4 -> 1, 5 -> 3, 6 -> 2).reduce(Unique[Int]) must_== Value(Set(1, 2, 3))
    }

    "return unique elements in sparse series" in {
      val s = Series.fromCells(1 -> Value("a"), 2 -> NA, 1 -> Value("b"), 3 -> NA)
      s.reduce(Unique[String]) must_== Value(Set("a", "b"))
    }

    "return unique elements by key" in {
      val s = Series.fromCells(
        1 -> Value("a"), 1 -> Value("b"),
        2 -> NA, 2 -> Value("c"),
        3 -> Value("d"), 3 -> NA, 3 -> Value("e"), 3 -> NA, 3 -> Value("e"),
        4 -> NA, 4 -> NA)
      s.reduceByKey(Unique[String]) must_== Series(1 -> Set("a", "b"), 2 -> Set("c"), 3 -> Set("d", "e"), 4 -> Set.empty)
    }
  }

  "SemigroupReducer" in {
    "return NA when empty" in {
      Series.empty[Int, String].reduce(SemigroupReducer[String]) must_== NA
      Series.fromCells[Int, String](1 -> NA, 2 -> NA).reduce(SemigroupReducer[String]) must_== NA
    }

    "reduce by key" in {
      val s = Series.fromCells(
        1 -> Value("a"), 1 -> Value("b"),
        2 -> NA, 2 -> Value("c"),
        3 -> Value("d"), 3 -> NA, 3 -> Value("e"), 3 -> NA, 3 -> Value("e"),
        4 -> NA, 4 -> NA)
      s.reduceByKey(SemigroupReducer[String]) must_==
        Series.fromCells(1 -> Value("ab"), 2 -> Value("c"), 3 -> Value("dee"), 4 -> NA)
    }
  }
}
