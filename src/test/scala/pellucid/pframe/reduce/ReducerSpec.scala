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

  object duplicate {
    val dense = Series("a" -> 1D, "a" -> 2D, "b" -> 3D, "b" -> 4D, "b" -> 5D, "c" -> 6D)
    val sparse = Series(
              Index.fromKeys("a",       "b", "b", "b",       "b", "b",       "c", "c",       "c",       "d"),
      Column.fromCells(Vector(NA, Value(2D),  NM,  NA, Value(4D),  NM, Value(5D),  NA, Value(1D), Value(0D))))
  }

  "Mean" should {
    "find mean of empty series" in {
      empty.reduce(Mean[Double]) must_== None
    }

    "find mean of dense series" in {
      unique.dense.reduce(Mean[Double]) must_== Some(3D)
      duplicate.dense.reduce(Mean[Double]) must_== Some(3.5)
    }

    "find mean of sparse series" in {
      unique.sparse.reduce(Mean[Double]) must_== Some(3D)
      duplicate.sparse.reduce(Mean[Double]) must_== Some(12D / 5D)
    }

    "find mean of dense series by key" in {
      duplicate.dense.reduceByKey(Mean[Double]) must_== Series("a" -> Some(1.5), "b" -> Some(4D), "c" -> Some(6D))
    }

    "find mean of sparse series by key" in {
      duplicate.sparse.reduceByKey(Mean[Double]) must_== Series("a" -> None, "b" -> Some(3D), "c" -> Some(3D), "d" -> Some(0D))
    }
  }

  "Sum" should {
    "sum empty series" in {
      empty.reduce(Sum[Double]) must_== 0D
    }

    "sum dense series" in {
      unique.dense.reduce(Sum[Double]) must_== 12D
      duplicate.dense.reduce(Sum[Double]) must_== 21D
    }

    "sum sparse series" in {
      unique.sparse.reduce(Sum[Double]) must_== 6D
      duplicate.sparse.reduce(Sum[Double]) must_== 12D
    }

    "sum dense series by key" in {
      duplicate.dense.reduceByKey(Sum[Double]) must_== Series("a" -> 3D, "b" -> 12D, "c" -> 6D)
    }

    "sum sparse series by key" in {
      duplicate.sparse.reduceByKey(Sum[Double]) must_== Series("a" -> 0D, "b" -> 6D, "c" -> 6D, "d" -> 0D)
    }
  }

  "Count" should {
    "count empty series" in {
      empty.reduce(Count) must_== 0
    }

    "count dense series" in {
      unique.dense.reduce(Count) must_== 4
      duplicate.dense.reduce(Count) must_== 6
    }

    "count sparse series" in {
      unique.sparse.reduce(Count) must_== 2
      duplicate.sparse.reduce(Count) must_== 5
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
      empty.reduce(Max[Double]) must_== None
    }

    "find max in dense series" in {
      unique.dense.reduce(Max[Double]) must_== Some(5D)
      duplicate.dense.reduce(Max[Double]) must_== Some(6D)
    }

    "find max in sparse series" in {
      unique.sparse.reduce(Max[Double]) must_== Some(4D)
      duplicate.sparse.reduce(Max[Double]) must_== Some(5D)
    }

    "find max in dense series by key" in {
      duplicate.dense.reduceByKey(Max[Double]) must_== Series("a" -> Some(2D), "b" -> Some(5D), "c" -> Some(6D))
    }

    "find max in sparse series by key" in {
      duplicate.sparse.reduceByKey(Max[Double]) must_== Series("a" -> None, "b" -> Some(4D), "c" -> Some(5D), "d" -> Some(0D))
    }
  }
}
