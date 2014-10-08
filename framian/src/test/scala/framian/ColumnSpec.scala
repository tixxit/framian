package framian
package column

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import spire.algebra._
import spire.syntax.monoid._

class ColumnSpec extends Specification {
  def slice[A](col: Column[A])(indices: Int*): Vector[Cell[A]] =
    indices.toVector map (col(_))

  "Column construction" should {
    "wrap arrays" in {
      val col = Column.dense(Array(1, 2, 3))
      slice(col)(-1, 0, 1, 2, 3) must_== Vector(NA, Value(1), Value(2), Value(3), NA)
    }

    "constructable from Cells" in {
      val col = Column(NM, NA, Value("a"), NM)
      slice(col)(-1, 0, 1, 2, 3, 4) must_== Vector(NA, NM, NA, Value("a"), NM, NA)
    }

    "wrap row function" in {
      val col = Column.eval(row => Value(-row))
      col(0) must_== Value(0)
      col(Int.MinValue) must_== Value(Int.MinValue)
      col(Int.MaxValue) must_== Value(-Int.MaxValue)
      col(5) must_== Value(-5)
    }

    "empty is empty" in {
      val col = Column.empty[String]()
      slice(col)(Int.MinValue, 0, Int.MaxValue, -1, 1, 200) must contain(be_==(NA)).forall
    }
  }

  "memo columns" should {
    "only evaluate values at most once" in {
      var counter = 0
      val col = Column.eval { row => counter += 1; Value(row) }.memoize()
      col(0); col(0); col(1); col(0); col(0); col(1); col(2); col(1); col(0)
      counter must_==(3)
    }
  }

  "Column" should {
    val col = Column(NM, NA, Value("a"))

    //"know which rows are values" in {
    //  col.isValueAt(0) must beFalse
    //  col.isValueAt(1) must beFalse
    //  col.isValueAt(2) must beTrue
    //  col.isValueAt(3) must beFalse
    //}

    //"return correct non value" in {
    //  col.nonValueAt(-1) must_== NA
    //  col.nonValueAt(0) must_== NM
    //  col.nonValueAt(1) must_== NA
    //  col.nonValueAt(3) must_== NA
    //}

    //"filter filters values to NA" in {
    //  val col0 = Column.fromArray(Array.range(0, 40))
    //  val col1 = col0 filter (_ % 2 == 0)
    //  (0 until 40 by 2) map (col1 isValueAt _) must contain(beTrue).forall
    //  (1 until 40 by 2) map (col1 nonValueAt _) must contain(be_==(NA)).forall
    //}

    //"map should map values" in {
    //  val col0 = Column.fromArray(Array(1, 2, 3, 4))
    //  val col1 = col0 map { x => x * x }
    //  slice(col1)(-1, 0, 1, 2, 3, 4) must_== Vector(NA, Value(1), Value(4), Value(9), Value(16), NA)
    //}

    //"masked column should mask rows" in {
    //  val col0 = Column.fromCells(Vector(NM, Value(1), Value(2), NM))
    //  val col1 = col0.mask(Set(2, 3))
    //  col1(0) must_== NA
    //  col1(1) must_== NA
    //  col1(2) must_== Value(2)
    //  col1(3) must_== NM
    //}

    //"mask infinite column" in {
    //  val col0 = Column(_.toString)
    //  val col1 = col0 mask (_ != 42)
    //  (-5 to 41) map (col1 isValueAt _) must contain(beTrue).forall
    //  (43 to 100) map (col1 isValueAt _) must contain(beTrue).forall
    //  col1(42) must_== NA
    //}

    //"reindex row with reindex" in {
    //  val col = Column.fromArray(Array.range(1, 5)).reindex((4 to 0 by -1).toArray)
    //  slice(col)(0, 1, 2, 3, 4) must_== Vector(NA, Value(4), Value(3), Value(2), Value(1))
    //}

    //"reindex turns values outside of range to NA" in {
    //  val col = Column.fromArray(Array.range(1, 5)).reindex((4 to 0 by -1).toArray)
    //  col(-1) must_== NA
    //  col(5) must_== NA
    //  col(6) must_== NA
    //  col(Int.MinValue) must_== NA
    //}
  }

  "Column Monoid" should {
    "have an empty id" in {
      val col = Monoid[Column[Int]].id
      slice(col)(Int.MinValue, 0, 1, 2, Int.MaxValue) must contain(be_==(NA)).forall
    }

    "be left biased" in {
      val a = Column.eval(row => Value(row))
      val b = Column.eval(row => Value(-row))
      (a |+| b)(1) must_== Value(1)
      (a |+| b)(2) must_== Value(2)
      (b |+| a)(1) must_== Value(-1)
      (b |+| a)(2) must_== Value(-2)
    }

    "ignore non values" in {
      //                                     0,        1,  2,  3,  4,  5,        6,        7
      val a = Column(Value(1), Value(2), NA, NA, NM, NM,       NA,       NM)
      val b = Column(      NA,       NM, NA, NM, NA, NM, Value(1), Value(2))
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

abstract class BaseColumnSpec extends Specification with ScalaCheck {
  import ColumnGenerators._

  implicit class ColumnOps[A](col: Column[A]) {
    def slice(rows: Seq[Int]): Vector[Cell[A]] = rows.map(col(_))(collection.breakOut)
  }

  def mkCol[A](cells: Cell[A]*): Column[A]

  "foreach" should {
    "bail early on NM values" in {
      val col = mkCol(Value(1), Value(2), NM, Value(4))
      col.foreach(0, 5, n => n) { (i, n) =>
        if (n == 4)
          throw new Exception()
      }
      ok
    }

    "skip NA values" in {
      val col = mkCol(Value(1), NA, Value(2), NA, Value(3))
      var bldr = List.newBuilder[Int]
      col.foreach(0, 5, n => 4 - n) { (i, n) =>
        bldr += n
      }
      bldr.result() must_== List(3, 2, 1)
    }
  }

  "shift" should {
    "move column rows up" in {
      val col = mkCol(Value(1), NA, NA, NM, Value(5))
      col.shift(2).slice(0 to 6) must_== Vector(NA, NA, Value(1), NA, NA, NM, Value(5))
      col.shift(-2).slice(-2 to 2) must_== Vector(Value(1), NA, NA, NM, Value(5))
    }
  }

  "reindex" should {
    "return empty column for empty array" in {
      val col = mkCol(Value(1), Value(2)).reindex(Array())
      (-10 to 10).map(col(_)).forall(_ must_== NA)
    }

    "reindex rearranges values" in {
      val col = mkCol(Value(1), Value(2), Value(3))
      col.reindex(Array(1, 2, 0)).slice(0 to 2) must_== Vector(Value(2), Value(3), Value(1))
    }

    "reindex rearranges NMs" in {
      val col = mkCol(Value(1), NM, Value(3), NM)
      col.reindex(Array(1, 2, 3, 0)).slice(0 to 3) must_== Vector(NM, Value(3), NM, Value(1))
    }

    "reindex with negative indices" in {
      val col = mkCol(Value(1), Value(2))
      col.reindex(Array(-1, 0, -1)).slice(0 to 2) must_== Vector(NA, Value(1), NA)
    }
  }

  val genMask: Gen[Mask] = for {
    rows0 <- arbitrary[List[Int]]
    rows = rows0.map(_ & 0xFF)
  } yield Mask(rows: _*)

  implicit val arbMask: Arbitrary[Mask] = Arbitrary(genMask)

  "mask" should {
    "turn all masked rows into NAs" in check { (col: Column[Int], mask: Mask) =>
      val masked = col.mask(mask)
      mask.toSet.forall(i => masked(i) == NA)
    }

    "retain unmasked rows as-is" in check { (col: Column[Int], rows: List[Int], mask: Mask) =>
      val validRows = rows.toSet -- mask.toSet
      val masked = col.mask(mask)
      validRows.forall(i => col(i) == masked(i))
    }
  }

  "setNA" should {
    "set the specified row to NA" in check { (col: Column[Int], row: Int) =>
      val col0 = col.setNA(row)
      col0(row) must_== NA
    }

    "not modify other rows" in check { (col: Column[Int], row: Int, rows: List[Int]) =>
      val validRows = rows.toSet - row
      val col0 = col.setNA(row)
      validRows.forall(i => col(i) == col0(i))
    }
  }

  "force" should {
    "return empty column when size is 0" in check { (col: Column[Int], indices: List[Int]) =>
      val empty = col.force(0)
      indices.map(empty(_)).forall(_ == NA)
    }

    "not mess with values in range" in check { (col: Column[Int], blen: Byte) =>
      val len = blen.toInt.abs
      col.force(len).slice(0 until len) must_== col.slice(0 until len)
    }

    "NA all values out of range" in check { (col: Column[Int], len: Int, indices: List[Int]) =>
      val len0 = len & 0x7FFFFFFF
      col.force(len0).slice(indices.filter(_ >= len0)).forall(_ == NA)
    }
  }
}

class DenseColumnSpec extends BaseColumnSpec {
  def mkCol[A](cells: Cell[A]*): Column[A] = Column(cells: _*)

  "dense columns" should {
    "manually spec from dense constructor" in {
      Column.dense(Array("1","2","3")) must beAnInstanceOf[GenericColumn[_]]
      Column.dense(Array(1,2,3)) must beAnInstanceOf[IntColumn]
      Column.dense(Array(1L,2L,3L)) must beAnInstanceOf[LongColumn]
      Column.dense(Array(1D,2D,3D)) must beAnInstanceOf[DoubleColumn]
    }

    "manually spec from default constructor" in {
      Column(NA, Value("x"), NM) must beAnInstanceOf[GenericColumn[_]]
      Column(NA, Value(1), NM) must beAnInstanceOf[IntColumn]
      Column(NA, Value(1L), NM) must beAnInstanceOf[LongColumn]
      Column(NA, Value(1D), NM) must beAnInstanceOf[DoubleColumn]
    }

    "use AnyColumn when type is not known and not-spec" in {
      def mkCol[A](as: A*): Column[A] = Column(as.map(Value(_)): _*)
      mkCol("x", "y") must beAnInstanceOf[AnyColumn[_]]
    }

    "use spec col when type is not known but spec" in {
      def mkCol[A](as: A*): Column[A] = Column(as.map(Value(_)): _*)
      mkCol(1, 2) must beAnInstanceOf[IntColumn]
      mkCol(1L, 2L) must beAnInstanceOf[LongColumn]
      mkCol(1D, 2D) must beAnInstanceOf[DoubleColumn]
    }

    "force manual spec through map" in {
      val col = Column.dense(Array("1","2","3"), Mask(1))
      col.map(_.toDouble) must beAnInstanceOf[DoubleColumn]
      col.map(_.toInt) must beAnInstanceOf[IntColumn]
      col.map(_.toLong) must beAnInstanceOf[LongColumn]
    }

    "force manual spec through flatMap" in {
      val col = Column.dense(Array("1","2","3"), Mask(1))
      col.flatMap(n => Value(n.toDouble)) must beAnInstanceOf[DoubleColumn]
      col.flatMap(n => Value(n.toInt)) must beAnInstanceOf[IntColumn]
      col.flatMap(n => Value(n.toLong)) must beAnInstanceOf[LongColumn]
    }

    "retain manual spec through filter" in {
      Column.dense(Array(1,2,3)).filter(_ == 2) must beAnInstanceOf[IntColumn]
      Column.dense(Array(1D,2D,3D)).filter(_ == 2) must beAnInstanceOf[DoubleColumn]
      Column.dense(Array(1L,2L,3L)).filter(_ == 2) must beAnInstanceOf[LongColumn]
    }

    "retain manual spec through orElse" in {
      Column(Value(1)).orElse(Column(Value(2))) must beAnInstanceOf[IntColumn]
      Column(Value(1L)).orElse(Column(Value(2L))) must beAnInstanceOf[LongColumn]
      Column(Value(1D)).orElse(Column(Value(2D))) must beAnInstanceOf[DoubleColumn]
    }

    "retain manual spec through reindex" in {
      Column.dense(Array(1,2,3)).reindex(Array(2,1,0)) must beAnInstanceOf[IntColumn]
      Column.dense(Array(1D,2D,3D)).reindex(Array(2,1,0)) must beAnInstanceOf[DoubleColumn]
      Column.dense(Array(1L,2L,3L)).reindex(Array(2,1,0)) must beAnInstanceOf[LongColumn]
    }

    "retain manual spec through force" in {
      Column.dense(Array(1,2,3)).force(2) must beAnInstanceOf[IntColumn]
      Column.dense(Array(1D,2D,3D)).force(2) must beAnInstanceOf[DoubleColumn]
      Column.dense(Array(1L,2L,3L)).force(2) must beAnInstanceOf[LongColumn]
    }

    "retain manual spec through mask" in {
      val mask = Mask(1)
      Column.dense(Array(1,2,3)).mask(mask) must beAnInstanceOf[IntColumn]
      Column.dense(Array(1D,2D,3D)).mask(mask) must beAnInstanceOf[DoubleColumn]
      Column.dense(Array(1L,2L,3L)).mask(mask) must beAnInstanceOf[LongColumn]
    }

    "retain manual spec through setNA" in {
      Column.dense(Array(1,2,3)).setNA(Int.MinValue) must beAnInstanceOf[IntColumn]
      Column.dense(Array(1D,2D,3D)).setNA(Int.MinValue) must beAnInstanceOf[DoubleColumn]
      Column.dense(Array(1L,2L,3L)).setNA(Int.MinValue) must beAnInstanceOf[LongColumn]
    }

    "setNA should be no-op when row already NA" in {
      val col = Column.dense(Array(1,2,3)).asInstanceOf[IntColumn]
      Column.dense(Array(1,2,3)).setNA(Int.MinValue).setNA(3).setNA(Int.MaxValue).asInstanceOf[IntColumn].naValues.max must_== None
      Column.dense(Array(1L,2L,3L)).setNA(Int.MinValue).setNA(3).setNA(Int.MaxValue).asInstanceOf[LongColumn].naValues.max must_== None
      Column.dense(Array(1D,2D,3D)).setNA(Int.MinValue).setNA(3).setNA(Int.MaxValue).asInstanceOf[DoubleColumn].naValues.max must_== None
    }
  }
}

class EvalColumnSpec extends BaseColumnSpec {
  def mkCol[A](cells: Cell[A]*): Column[A] = {
    val cells0 = cells.toVector
    Column.eval(row => if (row >= 0 && row < cells0.size) cells0(row) else NA)
  }

  "eval columns" should {
    "return dense columns from reindex" in {
      Column.eval(Value(_)).reindex(Array(1,3,2)) must beAnInstanceOf[DenseColumn[_]]
    }

    "return dense columns from force" in {
      Column.eval(Value(_)).force(5) must beAnInstanceOf[DenseColumn[_]]
    }
  }
}
