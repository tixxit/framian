package framian
package column

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import spire.algebra._
import spire.syntax.monoid._

class ColumnSpec extends Specification with ScalaCheck {
  def slice[A](col: Column[A])(indices: Int*): Vector[Cell[A]] =
    indices.toVector map (col(_))

  def genColumn[A](gen: Gen[A]): Gen[Column[A]] = for {
    cellValues <- Gen.listOf(CellGenerators.genCell(gen, (2, 1, 1)))
    dense <- arbitrary[Boolean]
  } yield {
    val col = Column(cellValues: _*)
    if (dense) col else Column.eval(row => col(row))
  }

  implicit def arbColumn[A: Arbitrary]: Arbitrary[Column[A]] = Arbitrary(genColumn(arbitrary[A]))

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

  "Monoid[Column[A]]" should {
    "left identity" in check { (col0: Column[Int], indices: List[Int]) =>
      val col1 = Monoid[Column[Int]].id orElse col0
      indices.map(col0(_)) must_== indices.map(col1(_))
    }

    "right identity" in check { (col0: Column[Int], indices: List[Int]) =>
      val col1 = col0 orElse Monoid[Column[Int]].id
      indices.map(col0(_)) must_== indices.map(col1(_))
    }

    "associative" in check { (a: Column[Int], b: Column[Int], c: Column[Int], indices: List[Int]) =>
      val col0 = ((a orElse b) orElse c)
      val col1 = (a orElse (b orElse c))
      indices.map(col0(_)) must_== indices.map(col1(_))
    }
  }
}

abstract class BaseColumnSpec extends Specification with ScalaCheck {
  implicit class ColumnOps[A](col: Column[A]) {
    def slice(rows: Seq[Int]): Vector[Cell[A]] = rows.map(col(_))(collection.breakOut)
  }

  def mkCol[A](cells: Cell[A]*): Column[A]

  def genColumn[A](gen: Gen[A]): Gen[Column[A]] = for {
    cellValues <- Gen.listOf(CellGenerators.genCell(gen, (2, 1, 1)))
  } yield mkCol(cellValues: _*)

  val genMask: Gen[Mask] = for {
    rows0 <- arbitrary[List[Int]]
    rows = rows0.map(_ & 0xFF)
  } yield Mask(rows: _*)

  implicit def arbColumn[A: Arbitrary]: Arbitrary[Column[A]] = Arbitrary(genColumn(arbitrary[A]))

  implicit val arbMask: Arbitrary[Mask] = Arbitrary(genMask)

  "foldRow" should {
    "return value for Value row" in {
      val col = mkCol(NA, Value(42))
      col.foldRow(1)(0, 0, a => 2 + a) must_== 44
    }

    "return NA value for NA row" in {
      val col = mkCol(NA, NA)
      col.foldRow(1)(true, false, _ => false) must beTrue
    }

    "return NM value for NM row" in {
      val col = mkCol(NA, NM)
      col.foldRow(1)(false, true, _ => false) must beTrue
    }

    "fold rows of columns" in check { (col: Column[Int], indices: List[Int]) =>
      val expected = indices.map(col(_)).map {
        case Value(a) => a.toString
        case NA => "NA"
        case NM => "NM"
      }
      val actual = indices.map(col.foldRow(_)("NA", "NM", _.toString))
      actual must_== expected
    }
  }

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

  "map" should {
    "map each cell in the column" in check { (col: Column[Int], indices: List[Int]) =>
      val f: Int => String = { n => (n * 23).toString }
      val col0 = col.map(f)
      indices.map(col(_)).map(_.map(f)) must_== indices.map(col0(_))
    }
  }

  "flatMap" should {
    "flatMap each cell in the column" in check { (col: Column[Int], indices: List[Int]) =>
      val f: Int => Cell[String] = { n =>
        if (n % 3 == 0) Value((n * 23).toString)
        else if (n % 3 == 1) NA
        else NM
      }
      val col0 = col.flatMap(f)
      indices.map(col(_)).map(_.flatMap(f)) must_== indices.map(col0(_))
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

    "NA all values out of range" in check { (col: Column[Int], len0: Int, indices: List[Int]) =>
      val len = len0 & 0x7FFFF
      col.force(len).slice(indices.filter(_ >= len)).forall(_ == NA)
    }
  }

  "shift" should {
    "shift all rows" in check { (col: Column[Int], rows0: Int, indices0: List[Int]) =>
      val rows = rows0 % 100 // Keep it sane for DenseColumns sake.
      val indices = indices0.filter(_ < Int.MaxValue - 100).filter(_ > Int.MinValue + 100)
      val shifted = col.shift(rows)
      indices.map(shifted(_)) must_== indices.map(_ - rows).map(col(_))
    }
  }

  "orElse" should {
    "be left biased" in {
      val a = mkCol(Value(0), Value(1), Value(2))
      val b = mkCol(Value(0), Value(-1), Value(-2))
      (a orElse b)(1) must_== Value(1)
      (a orElse b)(2) must_== Value(2)
      (b orElse a)(1) must_== Value(-1)
      (b orElse a)(2) must_== Value(-2)
    }

    "ignore non values" in {
      val a = mkCol(Value(1), Value(2), NA, NA, NM, NM,       NA,       NM)
      val b = mkCol(      NA,       NM, NA, NM, NA, NM, Value(1), Value(2))
      val col = a orElse b

      col(0) must_== Value(1)
      col(1) must_== Value(2)
      col(2) must_== NA
      col(3) must_== NM
      col(4) must_== NM
      col(5) must_== NM
      col(6) must_== Value(1)
      col(7) must_== Value(2)
    }
  }

  "memoize" should {
    "calculate values at most once (pessimistic)" in check { (col: Column[Int], indices: List[Int]) =>
      var hit = false
      val col0 = col.map { a => hit = true; a }.memoize(false)
      indices.map(col0(_))
      hit = false
      indices.map(col0(_))
      hit must_== false
    }

    "calculate values at most once (optimistic, no thread contention)" in check { (col: Column[Int], indices: List[Int]) =>
      var hit = false
      val col0 = col.map { a => hit = true; a }.memoize(true)
      indices.map(col0(_))
      hit = false
      indices.map(col0(_))
      hit must_== false
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

    "not overflow index on shift" in {
      Column.eval(Value(_)).shift(1)(Int.MinValue) must_== NA
    }
  }
}
