package framian

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._
import spire.std.iterable._

import shapeless._

class FrameSpec extends Specification with ScalaCheck {
  import Arbitrary.arbitrary
  import Prop._
  import FrameGenerators._

  val f0 = Frame.fromRows(
    "a" :: 1 :: HNil,
    "b" :: 2 :: HNil,
    "c" :: 3 :: HNil)
  val f1 = Frame.fromRows(
    "a" :: 3 :: HNil,
    "b" :: 2 :: HNil,
    "c" :: 1 :: HNil)
  val f2 = Frame.fromRows(
    "a" :: 1 :: HNil,
    "b" :: 2 :: HNil,
    "b" :: 3 :: HNil)

  val f3 = Series(
      1 -> 3,
      2 -> 2,
      2 -> 1
    ).toFrame(0)
  val f4 = Series(
      1 -> 3,
      2 -> 2,
      2 -> 1
    ).toFrame(1)
  val f5 = Series(
      2 -> 3,
      2 -> 2,
      3 -> 1
    ).toFrame(1)
  val f6 = Series(
      2 -> 2,
      2 -> 1
    ).toFrame(1)

  val s0 = Series(
    0 -> "s3",
    1 -> "s2",
    2 -> "s1")
  val s1 = Series(
    1 -> "s3",
    2 -> "s2",
    2 -> "s1")

  val homogeneous = Frame.fromRows(
    1.0  :: 2.0 :: 3.0  :: HNil,
    0.5  :: 1.0 :: 1.5  :: HNil,
    0.25 :: 0.5 :: 0.75 :: HNil
  )
  val people = Frame.fromRows(
      "Bob"     :: 32 :: "Manager"  :: HNil,
      "Alice"   :: 24 :: "Employee" :: HNil,
      "Charlie" :: 44 :: "Employee"  :: HNil)
    .withColIndex(Index.fromKeys("Name", "Age", "Level"))
    .withRowIndex(Index.fromKeys("Bob", "Alice", "Charlie"))

  "Frame" should {
    "be fill-able" in {
      val f = Frame.fill(1 to 3, 4 to 5) { (i, j) =>
        val k = i + j
        if (k % 2 == 0) NA else Value(k)
      }

      f must_== Frame.mergeColumns(
        4 -> Series.fromCells(1 -> Value(5), 2 ->       NA, 3 -> Value(7)),
        5 -> Series.fromCells(1 ->       NA, 2 -> Value(7), 3 ->       NA)
      )
    }

    "have sane equality" in {
      f0 must_== f0
      f0 must_!= f1
      f1 must_!= f0
      f0.column[String](0).toFrame("abc") must_== f1.column[String](0).toFrame("abc")
      f0.column[Int](1).toFrame("123") must_!= f1.column[Int](1).toFrame("123")
    }

    "have sane hashCode" in {
      f0.hashCode must_== f0.hashCode
      f0.hashCode must_!= f1.hashCode
      f1.hashCode must_!= f0.hashCode
      f0.column[String](0).toFrame("abc").hashCode must_== f1.column[String](0).toFrame("abc").hashCode
      f0.column[Int](1).toFrame("123").hashCode must_!= f1.column[Int](1).toFrame("123").hashCode
    }

    "sort columns" in {
      people.sortColumns must_== Frame.fromRows(
          32 :: "Manager"  :: "Bob" :: HNil,
          24 :: "Employee" :: "Alice" :: HNil,
          44 :: "Employee"  :: "Charlie" :: HNil)
        .withColIndex(Index.fromKeys("Age", "Level", "Name"))
        .withRowIndex(Index.fromKeys("Bob", "Alice", "Charlie"))
    }

    "sort rows" in {
      people.sortRows must_== Frame.fromRows(
          "Alice"   :: 24 :: "Employee" :: HNil,
          "Bob"     :: 32 :: "Manager"  :: HNil,
          "Charlie" :: 44 :: "Employee"  :: HNil)
        .withColIndex(Index.fromKeys("Name", "Age", "Level"))
        .withRowIndex(Index.fromKeys("Alice", "Bob", "Charlie"))
    }

    "use new row index" in {
      f0.withRowIndex(Index(0 -> 2, 1 -> 0, 2 -> 1)) must_== Frame.fromRows(
        "c" :: 3 :: HNil,
        "a" :: 1 :: HNil,
        "b" :: 2 :: HNil)
      f0.withRowIndex(Index(0 -> 0, 1 -> 0, 2 -> 0)) must_== Frame.fromRows(
        "a" :: 1 :: HNil,
        "a" :: 1 :: HNil,
        "a" :: 1 :: HNil)
      f0.withRowIndex(Index(0 -> 2)) must_== Frame.fromRows("c" :: 3 :: HNil)
      f0.withRowIndex(Index.empty[Int]) must_== Frame.empty[Int, Int].withColIndex(f0.colIndex)
    }

    "use new column index" in {
      f0.withColIndex(Index(0 -> 1, 1 -> 0)) must_== Frame.fromRows(
        1 :: "a" :: HNil,
        2 :: "b" :: HNil,
        3 :: "c" :: HNil)
      f0.withColIndex(Index(0 -> 0, 1 -> 0)) must_== Frame.fromRows(
        "a" :: "a" :: HNil,
        "b" :: "b" :: HNil,
        "c" :: "c" :: HNil)
      f0.withColIndex(Index(0 -> 1)) must_== Frame.fromRows(
        1 :: HNil,
        2 :: HNil,
        3 :: HNil)
      f0.withColIndex(Index.empty[Int]) must_== Frame.fromRows[HNil, Int](HNil, HNil, HNil)
    }

    "have trivial column/row representation for empty Frame" in {
      val frame = Frame.empty[String, String]
      frame.columnsAsSeries must_== Series.empty[String, UntypedColumn]
      frame.rowsAsSeries must_== Series.empty[String, UntypedColumn]
    }

    "be representable as columns" in {
      val series = f0.columnsAsSeries mapValues { col =>
        Series(f0.rowIndex, col.cast[Any])
      }

      series must_== Series(
        0 -> Series(0 -> "a", 1 -> "b", 2 -> "c"),
        1 -> Series(0 -> 1, 1 -> 2, 2 -> 3)
      )
    }

    "be representable as rows" in {
      val series = f0.rowsAsSeries mapValues { col =>
        Series(f0.colIndex, col.cast[Any])
      }

      series must_== Series(
        0 -> Series(0 -> "a", 1 -> 1),
        1 -> Series(0 -> "b", 1 -> 2),
        2 -> Series(0 -> "c", 1 -> 3)
      )
    }
  }

  "Frame merges" should {
    // these cases work as expected... tacking on a new column...
    "inner merge with frame of same row index" in {
      f3.merge(f4)(Merge.Inner) must_==
        Frame.fromRows(
          3 :: 3 :: HNil,
          2 :: 2 :: HNil,
          1 :: 1 :: HNil).withRowIndex(Index(Array(1,2,2)))
    }

    "outer merge with frame of same row index" in {
      f3.merge(f4)(Merge.Outer) must_==
        Frame.fromRows(
          3 :: 3 :: HNil,
          2 :: 2 :: HNil,
          1 :: 1 :: HNil
        ).withRowIndex(Index(Array(1,2,2)))
    }

    "inner merge with an offset index with duplicates" in {
      f3.merge(f5)(Merge.Inner) must_==
        Frame.fromRows(
          2 :: 3 :: HNil,
          1 :: 2 :: HNil
        ).withRowIndex(Index(Array(2, 2)))
    }

    "outer merge with an offset index with duplicates" in {
      f3.merge(f5)(Merge.Outer) must_==
        Frame.fromRows(
          3  :: NA :: HNil,
          2  :: 3  :: HNil,
          1  :: 2  :: HNil,
          NA :: 1  :: HNil
        ).withRowIndex(Index(Array(1,2,2,3)))
    }

    "inner merge with a smaller index with duplicates" in {
      f3.merge(f6)(Merge.Inner) must_==
        Frame.fromRows(
          2 :: 2 :: HNil,
          1 :: 1 :: HNil
        ).withRowIndex(Index(Array(2, 2)))
    }

    "outer merge with a smaller index with duplicates" in {
      f3.merge(f6)(Merge.Outer) must_==
        Frame.fromRows(
          3 :: NA :: HNil,
          2 :: 2  :: HNil,
          1 :: 1  :: HNil
        ).withRowIndex(Index(Array(1,2,2)))
    }

    "merge with a series" in {
      f3.merge(1, s1)(Merge.Inner) must_==
        Frame.fromRows(
          3 :: "s3" :: HNil,
          2 :: "s2" :: HNil,
          1 :: "s1" :: HNil
        ).withRowIndex(Index(Array(1,2,2)))
    }
  }

  "Frame joins" should {
    "inner join with empty frame" in {
      val e = Frame.empty[Int, Int]
      f0.join(e)(Join.Inner) must_== f0.withRowIndex(Index.empty[Int])
      e.join(f0)(Join.Inner) must_== f0.withRowIndex(Index.empty[Int])
      e.join(e)(Join.Inner) must_== e
    }

    "inner join with series" in {
      f0.join(2, s0)(Join.Inner) must_== Frame.fromRows(
        "a" :: 1 :: "s3" :: HNil,
        "b" :: 2 :: "s2" :: HNil,
        "c" :: 3 :: "s1" :: HNil)
    }

    "inner join with self" in {
      f0.join(f0)(Join.Inner) must_== Frame.fromRows(
          "a" :: 1 :: "a" :: 1 :: HNil,
          "b" :: 2 :: "b" :: 2 :: HNil,
          "c" :: 3 :: "c" :: 3 :: HNil)
        .withColIndex(Index.fromKeys(0, 1, 0, 1))
    }

    "inner join only matching rows" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val b = Frame.fromRows(2.0 :: HNil, 3.0 :: HNil)
        .withRowIndex(Index.fromKeys("b", "c"))
      val c = Frame.fromRows(2 :: 2.0 :: HNil)
        .withRowIndex(Index.fromKeys("b"))
        .withColIndex(Index.fromKeys(0, 0))

      a.join(b)(Join.Inner) must_== c
    }

    "inner join forms cross-product of matching rows" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "a"))
      val b = Frame.fromRows(2.0 :: HNil, 3.0 :: HNil)
        .withRowIndex(Index.fromKeys("a", "a"))
      val c = Frame.fromRows(
        1 :: 2.0 :: HNil,
        1 :: 3.0 :: HNil,
        2 :: 2.0 :: HNil,
        2 :: 3.0 :: HNil)
        .withRowIndex(Index.fromKeys("a", "a", "a", "a"))
        .withColIndex(Index.fromKeys(0, 0))

      a.join(b)(Join.Inner) must_== c
    }

    "left join keeps left mismatched rows" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val b = Frame.fromRows(2.0 :: HNil, 3.0 :: HNil)
        .withRowIndex(Index.fromKeys("b", "c"))
      val c = Frame.mergeColumns(
        0 -> Series.fromCells("a" -> Value(1), "b" -> Value(2)),
        0 -> Series.fromCells("a" ->       NA, "b" -> Value(2.0)))
      a.join(b)(Join.Left) must_== c
    }

    "left join with empty frame" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val e = Frame.empty[String, Int]
      a.join(e)(Join.Left) must_== a
      e.join(a)(Join.Left) must_== e.withColIndex(Index.fromKeys(0))
    }

    "right join keeps right mismatched rows" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val b = Frame.fromRows(2.0 :: HNil, 3.0 :: HNil)
        .withRowIndex(Index.fromKeys("b", "c"))
      val c = Frame.mergeColumns(
        0 -> Series.fromCells("b" -> Value(2), "c" -> NA),
        0 -> Series.fromCells("b" -> Value(2.0), "c" -> Value(3.0)))
      a.join(b)(Join.Right) must_== c
    }

    "right join with empty frame" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val e = Frame.empty[String, Int]
      a.join(e)(Join.Right) must_== e.withColIndex(Index.fromKeys(0))
      e.join(a)(Join.Right) must_== a
    }

    "outer join keeps all rows" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val b = Frame.fromRows(2.0 :: HNil, 3.0 :: HNil)
        .withRowIndex(Index.fromKeys("b", "c"))
      val c = Frame.mergeColumns(
        0 -> Series.fromCells("a" -> Value(1), "b" -> Value(2), "c" -> NA),
        0 -> Series.fromCells("a" -> NA, "b" -> Value(2.0), "c" -> Value(3.0)))
      a.join(b)(Join.Outer) must_== c
    }

    "outer join with empty frame" in {
      val a = Frame.fromRows(1 :: HNil, 2 :: HNil)
        .withRowIndex(Index.fromKeys("a", "b"))
      val e = Frame.empty[String, Int]
      a.join(e)(Join.Outer) must_== a
      e.join(a)(Join.Outer) must_== a
    }
  }

  "mapRowGroups" should {
    "not modify frame for identity" in {
      f0.mapRowGroups { (_, f) => f } must_== f0
      f1.mapRowGroups { (_, f) => f } must_== f1
    }

    val dups = Frame.fromRows(
      1 :: 2.0 :: HNil,
      2 :: 0.5 :: HNil,
      3 :: 1.0 :: HNil,
      4 :: 1.0 :: HNil,
      5 :: 8.9 :: HNil,
      6 :: 9.2 :: HNil
    ).withRowIndex(Index.fromKeys("a", "a", "b", "c", "c", "c"))

    "reduce groups" in {
      dups.mapRowGroups { (row, f) =>
        val reduced = f.reduceFrame(reduce.Sum[Double]).to[List]
        ColOrientedFrame(Index.fromKeys(row), Series(reduced map { case (key, value) =>
          key -> TypedColumn(Column(value))
        }: _*))
      } must_== dups.reduceFrameByKey(reduce.Sum[Double])
    }

    "replace groups with constant" in {
      val const = Frame.fromRows("repeat" :: HNil)
      dups.mapRowGroups { (_, f) => const } must_== Frame.fromRows(
        "repeat" :: HNil,
        "repeat" :: HNil,
        "repeat" :: HNil
      ).withRowIndex(Index.fromKeys(0, 0, 0))
    }
  }

  "Frame" should {
    "get row as HList" in {
      f0.get(Cols(0, 1).as[String :: Int :: HNil])(0) must_== Value("a" :: 1 :: HNil)
      f0.get(Cols(0, 1).as[String :: Int :: HNil])(1) must_== Value("b" :: 2 :: HNil)
      f0.get(Cols(0, 1).as[String :: Int :: HNil])(2) must_== Value("c" :: 3 :: HNil)
      f0.get(Cols(0, 1).as[String :: Int :: HNil])(3) must_== NA
      f0.get(Cols(0).as[String :: HNil])(0) must_== Value("a" :: HNil)
      f0.get(Cols(1).as[Int :: HNil])(2) must_== Value(3 :: HNil)
    }

    "convert to series" in {
      f0.get(Cols(0).as[String]) must_== Series(0 -> "a", 1 -> "b", 2 -> "c")
      f0.get(Cols(0).as[Int]) must_== Series(Index.fromKeys(0, 1, 2), Column[Int](NM, NM, NM))
      f0.get(Cols(1).as[Int]) must_== Series(0 -> 1, 1 -> 2, 2 -> 3)
      f0.get(Cols(0, 1).as[String :: Int :: HNil]) must_== Series(
        0 -> ("a" :: 1 :: HNil),
        1 -> ("b" :: 2 :: HNil),
        2 -> ("c" :: 3 :: HNil))
    }

    "map to series" in {
      f0.map(Cols(1).as[Int], 2)(_ + 1) must_== Frame.fromRows(
        "a" :: 1 :: 2 :: HNil,
        "b" :: 2 :: 3 :: HNil,
        "c" :: 3 :: 4 :: HNil)
      f0.map(Cols(0).as[String], 2)(_ => 42) must_== Frame.fromRows(
        "a" :: 1 :: 42 :: HNil,
        "b" :: 2 :: 42 :: HNil,
        "c" :: 3 :: 42 :: HNil)
      f0.map(Cols(1, 0).as[(Int, String)], 2) { case (x, y) =>
        y + x
      } must_== Frame.fromRows(
        "a" :: 1 :: "a1" :: HNil,
        "b" :: 2 :: "b2" :: HNil,
        "c" :: 3 :: "c3" :: HNil)
    }

    "map with index to series" in {
      f0.mapWithIndex(Cols(0).as[String], 2)(_ + _) must_== Frame.fromRows(
        "a" :: 1 :: "0a" :: HNil,
        "b" :: 2 :: "1b" :: HNil,
        "c" :: 3 :: "2c" :: HNil)
      f0.mapWithIndex(Cols(1).as[Int], 2)(_ + _) must_== Frame.fromRows(
        "a" :: 1 :: 1 :: HNil,
        "b" :: 2 :: 3 :: HNil,
        "c" :: 3 :: 5 :: HNil)
    }

    "filter whole frame" in {
      f0.filter(Cols(1).as[Int])(_ % 2 == 0) must_==
        Frame.fromRows("b" :: 2 :: HNil).withRowIndex(Index.fromKeys(1))
    }

    "group by column values" in {
      f0.group(Cols(0).as[String]) must_== f0.withRowIndex(Index.fromKeys("a", "b", "c"))
      f0.group(Cols(1).as[Int].map(-_)) must_== f0.withRowIndex(Index(-3 -> 2, -2 -> 1, -1 -> 0))
      f2.group(Cols(0).as[String]) must_== f2.withRowIndex(Index(("a",0), ("b",2), ("b",1)))
    }
  }

  "reduceFrameWithCol" should {
    "reduce with last" in {
      f0.reduceFrameWithCol[String, Int, (String, Int)](0)(reduce.Last) must_==
        Series(1 -> ("c", 3))
    }
  }

  "map" should {
    "append column when to is new" in {
      f0.map(Cols(1).as[Int], to = 2)(_ + 2) must_== Frame.fromRows(
        "a" :: 1 :: 3 :: HNil,
        "b" :: 2 :: 4 :: HNil,
        "c" :: 3 :: 5 :: HNil)
    }

    "replace column when `to` exists" in {
      f0.map(Cols(1).as[Int], to = 1)(_ + 2) must_== Frame.fromRows(
        "a" :: 3 :: HNil,
        "b" :: 4 :: HNil,
        "c" :: 5 :: HNil)
    }
  }

  "reduce" should {
    "reduce rows" in {
      f0.reduce(Cols(1).as[Double], 2)(reduce.Mean) must_==
        f0.merge(2, Series(0 -> 2D, 1 -> 2D, 2 -> 2D))(Merge.Outer)
    }

    "reduce cols" in {
      f0.transpose.reduce(Rows(1).as[Double], 2)(reduce.Mean) must_==
        f0.merge(2, Series(0 -> 2D, 1 -> 2D, 2 -> 2D))(Merge.Outer).transpose
    }

    "replace column when `to` exists" in {
      f0.reduce(Cols(1).as[Int], to = 1)(reduce.Sum) must_== Frame.fromRows(
        "a" :: 6 :: HNil,
        "b" :: 6 :: HNil,
        "c" :: 6 :: HNil)

      f0.transpose.reduce(Rows(1).as[Int], to = 1)(reduce.Sum) must_== Frame.fromColumns(
        "a" :: 6 :: HNil,
        "b" :: 6 :: HNil,
        "c" :: 6 :: HNil)
    }

    "respect NMs in reducer" in {
      val f = Series.fromCells(1 -> Value(1), 2 -> NM, 3 -> Value(3)).toFrame("x")
      f.reduce(Cols("x").as[Int], "y")(reduce.Sum) must_==
        f.merge("y", Series(1 -> NM, 2 -> NM, 3 -> NM))(Merge.Outer)
    }

    "respect NAs in reducer" in {
      val f = Series.fromCells[Int, Int](1 -> NA, 2 -> NA).toFrame("x")
      f.reduce(Cols("x").as[Int], "y")(reduce.Max) must_==
        f.merge("y", Series(1 -> NA, 2 -> NA))(Merge.Outer)
    }
  }

  "reduceByKey" should {
    val f = Frame.mergeColumns(
      "x" -> Series.fromCells(0 -> Value(1), 2 -> Value(5), 2 -> Value(6)),
      "y" -> Series.fromCells(0 -> Value(2), 0 -> Value(3), 1 -> NM, 1 -> Value(2)))

    "reduce rows/cols" in {
      f.reduceByKey(Cols("x").as[Int], "z")(reduce.Sum) must_==
        f.join("z", Series.fromCells(0 -> Value(1), 1 -> Value(0), 2 -> Value(11)))(Join.Outer)

      f.transpose.reduceByKey(Rows("x").as[Int], "z")(reduce.Sum) must_==
        f.join("z", Series.fromCells(0 -> Value(1), 1 -> Value(0), 2 -> Value(11)))(Join.Outer).transpose
    }

    "respect NMs from reducer" in {
      f.reduceByKey(Cols("y").as[Int], "z")(reduce.Sum) must_==
        f.join("z", Series.fromCells(0 -> Value(5), 1 -> NM, 2 -> Value(0)))(Join.Outer)
    }

    "respect NAs from reducer" in {
      f.reduceByKey(Cols("x").as[Int], "z")(reduce.Max) must_==
        f.join("z", Series.fromCells(0 -> Value(1), 1 -> NA, 2 -> Value(6)))(Join.Outer)
    }
  }

  "appendRows" should {
    "append rows to empty frame" in {
      Frame.empty[Int, Int].appendRows(f0) must_== f0
      f0.appendRows(Frame.empty[Int, Int]) must_== f0
    }

    "append 2 simple frames with same columns" in {
      f0.appendRows(f1) must_== Frame.fromRows(
        "a" :: 1 :: HNil,
        "b" :: 2 :: HNil,
        "c" :: 3 :: HNil,
        "a" :: 3 :: HNil,
        "b" :: 2 :: HNil,
        "c" :: 1 :: HNil
      ).withRowIndex(Index(Array(0, 1, 2, 0, 1, 2)))
    }

    "append 2 simple frames with different columns" in {
      val a = Frame.fromRows(
        "a" :: 1 :: HNil,
        "b" :: 2 :: HNil,
        "c" :: 3 :: HNil)
      val b = Frame.fromRows(
        9 :: 4D ::HNil,
        8 :: 5D ::HNil,
        7 :: 6D ::HNil).withColIndex(Index(Array(1, 2)))

      val col0 = Column(Value("a"), Value("b"), Value("c"), NA, NA, NA)
      val col1 = Column(Value(1), Value(2), Value(3), Value(9), Value(8), Value(7))
      val col2 = Column(NA, NA, NA, Value(4D), Value(5D), Value(6D))
      a.appendRows(b) must_== ColOrientedFrame(Index(Array(0, 1, 2, 0, 1, 2)),
        Series(0 -> TypedColumn(col0), 1 -> TypedColumn(col1), 2 -> TypedColumn(col2)))
    }

    "append frame rows with same column oriented schema" in {
      val genFrame = genColOrientedFrame[Int, String](arbitrary[Int])(
        "a" -> arbitrary[String],
        "b" -> arbitrary[Int],
        "c" -> arbitrary[Double])

      forAll(Gen.zip(genFrame, genFrame)) { case (f0, f1) =>
        val rows0 = f0.get(Cols("a", "b", "c").as[(String, Int, Double)])
        val rows1 = f1.get(Cols("a", "b", "c").as[(String, Int, Double)])
        val index = Index(rows0.index.keys ++ rows1.index.keys)
        val values = rows0.values ++ rows1.values
        val expected = Frame.fromRows(values: _*).withRowIndex(index).withColIndex(Index(Array("a", "b", "c")))
        f0.appendRows(f1) must_== expected
      }
    }
  }
}
