package pellucid.pframe

import org.specs2.mutable._

import spire.algebra._
import spire.syntax.monoid._

class ColumnSpec extends Specification {
  def slice[A](col: Column[A])(indices: Int*): Vector[Cell[A]] =
    indices.toVector map (col(_))

  "Column construction" should {
    "wrap arrays" in {
      val col = Column.fromArray(Array(1, 2, 3))
      slice(col)(-1, 0, 1, 2, 3) must_== Vector(NA, Value(1), Value(2), Value(3), NA)
    }

    "wrap seq of Cells" in {
      val col = Column.fromCells(Vector(NM, NA, Value("a"), NM))
      slice(col)(-1, 0, 1, 2, 3, 4) must_== Vector(NA, NM, NA, Value("a"), NM, NA)
    }

    "default constructor" in {
      val col = Column(row => -row)
      col.valueAt(0) must_== 0
      col.valueAt(Int.MinValue) must_== Int.MinValue
      col.valueAt(Int.MaxValue) must_== -Int.MaxValue
      col.valueAt(5) must_== -5
    }

    "empty is empty" in {
      val col = Column.empty[String]
      slice(col)(Int.MinValue, 0, Int.MaxValue, -1, 1, 200) must contain(be_==(NA)).forall
    }
  }

  "Column" should {
    val col = Column.fromCells(Vector(NM, NA, Value("a")))

    "know which rows are values" in {
      col.isValueAt(0) must beFalse
      col.isValueAt(1) must beFalse
      col.isValueAt(2) must beTrue
      col.isValueAt(3) must beFalse
    }

    "return correct non value" in {
      col.nonValueAt(-1) must_== NA
      col.nonValueAt(0) must_== NM
      col.nonValueAt(1) must_== NA
      col.nonValueAt(3) must_== NA
    }

    "filter filters values to NA" in {
      val col0 = Column.fromArray(Array.range(0, 40))
      val col1 = col0 filter (_ % 2 == 0)
      (0 until 40 by 2) map (col1 isValueAt _) must contain(beTrue).forall
      (1 until 40 by 2) map (col1 nonValueAt _) must contain(be_==(NA)).forall
    }

    "map should map values" in {
      val col0 = Column.fromArray(Array(1, 2, 3, 4))
      val col1 = col0 map { x => x * x }
      slice(col1)(-1, 0, 1, 2, 3, 4) must_== Vector(NA, Value(1), Value(4), Value(9), Value(16), NA)
    }

    "masked column should mask rows" in {
      val col0 = Column.fromCells(Vector(NM, Value(1), Value(2), NM))
      val col1 = col0.mask(Set(2, 3))
      col1(0) must_== NA
      col1(1) must_== NA
      col1(2) must_== Value(2)
      col1(3) must_== NM
    }

    "mask infinite column" in {
      val col0 = Column(_.toString)
      val col1 = col0 mask (_ != 42)
      (-5 to 41) map (col1 isValueAt _) must contain(beTrue).forall
      (43 to 100) map (col1 isValueAt _) must contain(beTrue).forall
      col1(42) must_== NA
    }

    "reindex row with reindex" in {
      val col = Column.fromArray(Array.range(1, 5)).reindex((4 to 0 by -1).toArray)
      slice(col)(0, 1, 2, 3, 4) must_== Vector(NA, Value(4), Value(3), Value(2), Value(1))
    }

    "reindex turns values outside of range to NA" in {
      val col = Column.fromArray(Array.range(1, 5)).reindex((4 to 0 by -1).toArray)
      col(-1) must_== NA
      col(5) must_== NA
      col(6) must_== NA
      col(Int.MinValue) must_== NA
    }
  }

  "Column Monoid" should {
    "have an empty id" in {
      val col = Monoid[Column[Int]].id
      slice(col)(Int.MinValue, 0, 1, 2, Int.MaxValue) must contain(be_==(NA)).forall
    }

    "be right biased" in {
      val a = Column(row => row)
      val b = Column(row => -row)
      (a |+| b).valueAt(1) must_== -1
      (a |+| b).valueAt(2) must_== -2
      (b |+| a).valueAt(1) must_== 1
      (b |+| a).valueAt(2) must_== 2
    }

    "ignore non values" in {
      //                                     0,        1,  2,  3,  4,  5,        6,        7
      val a = Column.fromCells(Vector(Value(1), Value(2), NA, NA, NM, NM,       NA,       NM))
      val b = Column.fromCells(Vector(      NA,       NM, NA, NM, NA, NM, Value(1), Value(2)))
      val col = a |+| b

      col(0) must_== Value(1)
      col(1) must_== Value(2)
      col(2) must_== NA
      col(3) must_== NM
      col(4) must_== NM
      col(5) must_== NM
      col(6) must_== Value(1)
      col(7) must_== Value(2)
    }
    
    // TODO: ScalaCheck tests for monoid properties. Spire provides this.
  }
}
