package framian
package column

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import org.scalatest.prop.{ Checkers, PropertyChecks }

import spire.algebra._
import spire.syntax.monoid._

class ColumnSpec extends FramianSpec
  with PropertyChecks
  with Checkers {

  def slice[A](col: Column[A])(indices: Int*): Vector[Cell[A]] =
    indices.toVector map (col(_))

  def mkEval[A](col: Column[A]): Column[A] = Column.eval(row => col(row))

  def genColumn[A](gen: Gen[A]): Gen[Column[A]] = for {
    cellValues <- Gen.listOf(CellGenerators.genCell(gen, (2, 1, 1)))
    dense <- arbitrary[Boolean]
  } yield {
    val col = Column(cellValues: _*)
    if (dense) col else mkEval(col)
  }

  implicit def arbColumn[A: Arbitrary]: Arbitrary[Column[A]] = Arbitrary(genColumn(arbitrary[A]))

  "Column construction" should {
    "wrap arrays" in {
      val col = Column.dense(Array(1, 2, 3))
      slice(col)(-1, 0, 1, 2, 3) should === (Vector(NA, Value(1), Value(2), Value(3), NA))
    }

    "constructable from Cells" in {
      val col = Column(NM, NA, Value("a"), NM)
      slice(col)(-1, 0, 1, 2, 3, 4) should === (Vector(NA, NM, NA, Value("a"), NM, NA))
    }

    "wrap row function" in {
      val col = Column.eval(row => Value(-row))
      col(0) should === (Value(0))
      col(Int.MinValue) should === (Value(Int.MinValue))
      col(Int.MaxValue) should === (Value(-Int.MaxValue))
      col(5) should === (Value(-5))
    }

    "empty is empty" in {
      val col = Column.empty[String]()
      slice(col)(Int.MinValue, 0, Int.MaxValue, -1, 1, 200).forall(_ == NA) shouldBe true
    }
  }

  "memo columns" should {
    "only evaluate values at most once" in {
      var counter = 0
      val col = Column.eval { row => counter += 1; Value(row) }.memoize()
      col(0); col(0); col(1); col(0); col(0); col(1); col(2); col(1); col(0)
      counter should === (3)
    }
  }

  "orElse with longer right side and NM" in {
    val b = Column[Int]()
    val c = Column[Int](NA, NM, NM)
    (b orElse c)(1) should === (NM)
  }

  "Monoid[Column[A]]" should {
    "left identity" in forAll { (col0: Column[Int], indices: List[Int]) =>
      val col1 = Monoid[Column[Int]].id orElse col0
      indices.map(col0(_)) should === (indices.map(col1(_)))
    }

    "right identity" in forAll { (col0: Column[Int], indices: List[Int]) =>
      val col1 = col0 orElse Monoid[Column[Int]].id
      indices.map(col0(_)) should === (indices.map(col1(_)))
    }

    "associative" in check {
      Prop.forAllNoShrink { (a: Column[Int], b: Column[Int], c: Column[Int], indices: List[Int]) =>
        val col0 = ((a orElse b) orElse c)
        val col1 = (a orElse (b orElse c))
        Prop(indices.map(col0(_)) == (indices.map(col1(_))))
      }
    }
  }
}

abstract class BaseColumnSpec extends FramianSpec with PropertyChecks {
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
      col.foldRow(1)(0, 0, a => 2 + a) should === (44)
    }

    "return NA value for NA row" in {
      val col = mkCol(NA, NA)
      col.foldRow(1)(true, false, _ => false) shouldBe true
    }

    "return NM value for NM row" in {
      val col = mkCol(NA, NM)
      col.foldRow(1)(false, true, _ => false) shouldBe true
    }

    "fold rows of columns" in forAll { (col: Column[Int], indices: List[Int]) =>
      val expected = indices.map(col(_)).map {
        case Value(a) => a.toString
        case NA => "NA"
        case NM => "NM"
      }
      val actual = indices.map(col.foldRow(_)("NA", "NM", _.toString))
      actual should === (expected)
    }
  }

  "foreach" should {
    "bail early on NM values" in {
      val col = mkCol(Value(1), Value(2), NM, Value(4))
      noException should be thrownBy col.foreach(0, 5, n => n) { (i, n) =>
        if (n == 4)
          throw new Exception()
      }
    }

    "skip NA values" in {
      val col = mkCol(Value(1), NA, Value(2), NA, Value(3))
      var bldr = List.newBuilder[Int]
      col.foreach(0, 5, n => 4 - n) { (i, n) =>
        bldr += n
      }
      bldr.result() should === (List(3, 2, 1))
    }

    "work with functions that can't be inlined" in {
      val col = mkCol(Value(1), NA, Value(2), NA, Value(3))
      var bldr = List.newBuilder[Int]
      val f: (Int, Int) => Unit = { (i, n) => bldr += n }
      col.foreach(0, 5, n => 4 - n)(f)
      bldr.result() should === (List(3, 2, 1))
    }
  }

  "map" should {
    "map each cell in the column" in forAll { (col: Column[Int], indices: List[Int]) =>
      val f: Int => String = { n => (n * 23).toString }
      val col0 = col.map(f)
      indices.map(col(_)).map(_.map(f)) should === (indices.map(col0(_)))
    }
  }

  "flatMap" should {
    "flatMap each cell in the column" in forAll { (col: Column[Int], indices: List[Int]) =>
      val f: Int => Cell[String] = { n =>
        if (n % 3 == 0) Value((n * 23).toString)
        else if (n % 3 == 1) NA
        else NM
      }
      val col0 = col.flatMap(f)
      indices.map(col(_)).map(_.flatMap(f)) should === (indices.map(col0(_)))
    }
  }

  "reindex" should {
    "return empty column for empty array" in {
      val col = mkCol(Value(1), Value(2)).reindex(Array())
      (-10 to 10).map(col(_)).forall(_ == NA) shouldBe true
    }

    "reindex rearranges values" in {
      val col = mkCol(Value(1), Value(2), Value(3))
      col.reindex(Array(1, 2, 0)).slice(0 to 2) should === (Vector(Value(2), Value(3), Value(1)))
    }

    "reindex rearranges NMs" in {
      val col = mkCol(Value(1), NM, Value(3), NM)
      col.reindex(Array(1, 2, 3, 0)).slice(0 to 3) should === (Vector(NM, Value(3), NM, Value(1)))
    }

    "reindex with negative indices" in {
      val col = mkCol(Value(1), Value(2))
      col.reindex(Array(-1, 0, -1)).slice(0 to 2) should === (Vector(NA, Value(1), NA))
    }
  }

  "mask" should {
    "turn all masked rows into NAs" in forAll { (col: Column[Int], mask: Mask) =>
      val masked = col.mask(mask)
      mask.toSet.forall(i => masked(i) == NA) shouldBe true
    }

    "retain unmasked rows as-is" in forAll { (col: Column[Int], rows: List[Int], mask: Mask) =>
      val validRows = rows.toSet -- mask.toSet
      val masked = col.mask(mask)
      validRows.forall(i => col(i) == masked(i)) shouldBe true
    }
  }

  "setNA" should {
    "set the specified row to NA" in forAll { (col: Column[Int], row: Int) =>
      val col0 = col.setNA(row)
      col0(row) should === (NA)
    }

    "not modify other rows" in forAll { (col: Column[Int], row: Int, rows: List[Int]) =>
      val validRows = rows.toSet - row
      val col0 = col.setNA(row)
      validRows.forall(i => col(i) == col0(i)) shouldBe true
    }
  }

  "force" should {
    "return empty column when size is 0" in forAll { (col: Column[Int], indices: List[Int]) =>
      val empty = col.force(0)
      indices.map(empty(_)).forall(_ == NA) shouldBe true
    }

    "not mess with values in range" in forAll { (col: Column[Int], blen: Byte) =>
      val len = blen.toInt.abs
      col.force(len).slice(0 until len) should === (col.slice(0 until len))
    }

    "NA all values out of range" in forAll { (col: Column[Int], len0: Int, indices: List[Int]) =>
      val len = len0 & 0x7FFFF
      col.force(len).slice(indices.filter(_ >= len)).forall(_ == NA) shouldBe true
    }
  }

  "shift" should {
    "shift all rows" in forAll { (col: Column[Int], rows0: Int, indices0: List[Int]) =>
      val rows = rows0 % 100 // Keep it sane for DenseColumns sake.
      val indices = indices0.filter(_ < Int.MaxValue - 100).filter(_ > Int.MinValue + 100)
      val shifted = col.shift(rows)
      indices.map(shifted(_)) should === (indices.map(_ - rows).map(col(_)))
    }
  }

  "orElse" should {
    "be left biased" in {
      val a = mkCol(Value(0), Value(1), Value(2))
      val b = mkCol(Value(0), Value(-1), Value(-2))
      (a orElse b)(1) should === (Value(1))
      (a orElse b)(2) should === (Value(2))
      (b orElse a)(1) should === (Value(-1))
      (b orElse a)(2) should === (Value(-2))
    }

    "ignore non values" in {
      val a = mkCol(Value(1), Value(2), NA, NA, NM, NM,       NA,       NM)
      val b = mkCol(      NA,       NM, NA, NM, NA, NM, Value(1), Value(2))
      val col = a orElse b

      col(0) should === (Value(1))
      col(1) should === (Value(2))
      col(2) should === (NA)
      col(3) should === (NM)
      col(4) should === (NM)
      col(5) should === (NM)
      col(6) should === (Value(1))
      col(7) should === (Value(2))
    }
  }

  "zipMap" should {
    "promote all NAs with spec type" in {
      val na = mkCol[Int](NA)
      val nm = mkCol[Int](NM)
      val value = mkCol[Int](Value(1))

      na.zipMap(na)(_ + _)(0) should === (NA)
      na.zipMap(nm)(_ + _)(0) should === (NA)
      nm.zipMap(na)(_ + _)(0) should === (NA)
      nm.zipMap(value)(_ + _)(0) should === (NM)
      value.zipMap(na)(_ + _)(0) should === (NA)
    }

    "promote all NAs with unspec type" in {
      val na = mkCol[String](NA)
      val nm = mkCol[String](NM)
      val value = mkCol[String](Value("x"))

      na.zipMap(na)(_ + _)(0) should === (NA)
      na.zipMap(nm)(_ + _)(0) should === (NA)
      nm.zipMap(na)(_ + _)(0) should === (NA)
      nm.zipMap(value)(_ + _)(0) should === (NM)
      value.zipMap(na)(_ + _)(0) should === (NA)
    }

    "NM if both are NM" in {
      val nm0 = mkCol[String](NM)
      val nm1 = mkCol[Int](NM)

      nm0.zipMap(nm1)(_ + _)(0) should === (NM)
      nm1.zipMap(nm0)(_ + _)(0) should === (NM)
    }

    "apply function if both are values" in {
      val col0 = mkCol(Value(1), NA, NM)
      val col1 = mkCol(Value(3D), Value(2D), NA)
      val col2 = mkCol(NA, Value("x"), NM)

      col0.zipMap(col0)(_ + _).slice(0 to 3) should === (Vector(Value(2), NA, NM, NA))
      col0.zipMap(col1)(_ + _).slice(0 to 3) should === (Vector(Value(4D), NA, NA, NA))
      col0.zipMap(col2)(_ + _).slice(0 to 3) should === (Vector(NA, NA, NM, NA))
      col1.zipMap(col0)(_ + _).slice(0 to 3) should === (Vector(Value(4D), NA, NA, NA))
      col1.zipMap(col1)(_ + _).slice(0 to 3) should === (Vector(Value(6D), Value(4D), NA, NA))
      col1.zipMap(col2)(_ + _).slice(0 to 3) should === (Vector(NA, Value("2.0x"), NA, NA))
      col2.zipMap(col0)(_ + _).slice(0 to 3) should === (Vector(NA, NA, NM, NA))
      col2.zipMap(col1)(_ + _).slice(0 to 3) should === (Vector(NA, Value("x2.0"), NA, NA))
      col2.zipMap(col2)(_ + _).slice(0 to 3) should === (Vector(NA, Value("xx"), NM, NA))
    }

    "conform to same semantics as Cell#zipMap" in forAll { (a: Column[Int], b: Column[Double], indices: List[Int]) =>
      val col = a.zipMap(b)(_ + _)
      indices.map(col(_)) should === (indices.map { row => a(row).zipMap(b(row))(_ + _) })
    }
  }

  "memoize" should {
    "calculate values at most once (pessimistic)" in forAll { (col: Column[Int], indices: List[Int]) =>
      var hit = false
      val col0 = col.map { a => hit = true; a }.memoize(false)
      indices.map(col0(_))
      hit = false
      indices.map(col0(_))
      hit should === (false)
    }

    "calculate values at most once (optimistic, no thread contention)" in forAll { (col: Column[Int], indices: List[Int]) =>
      var hit = false
      val col0 = col.map { a => hit = true; a }.memoize(true)
      indices.map(col0(_))
      hit = false
      indices.map(col0(_))
      hit should === (false)
    }
  }
}

class DenseColumnSpec extends BaseColumnSpec {
  def mkCol[A](cells: Cell[A]*): Column[A] = Column(cells: _*)

  "dense columns" should {
    "manually spec from dense constructor" in {
      Column.dense(Array("1","2","3")) shouldBe a [GenericColumn[_]]
      Column.dense(Array(1,2,3)) shouldBe an [IntColumn]
      Column.dense(Array(1L,2L,3L)) shouldBe a [LongColumn]
      Column.dense(Array(1D,2D,3D)) shouldBe a [DoubleColumn]
    }

    "manually spec from default constructor" in {
      Column(NA, Value("x"), NM) shouldBe a [GenericColumn[_]]
      Column(NA, Value(1), NM) shouldBe an [IntColumn]
      Column(NA, Value(1L), NM) shouldBe a [LongColumn]
      Column(NA, Value(1D), NM) shouldBe a [DoubleColumn]
    }

    "use AnyColumn when type is not known and not-spec" in {
      def mkCol[A](as: A*): Column[A] = Column(as.map(Value(_)): _*)
      mkCol("x", "y") shouldBe an [AnyColumn[_]]
    }

    "use spec col when type is not known but spec" in {
      def mkCol[A](as: A*): Column[A] = Column(as.map(Value(_)): _*)
      mkCol(1, 2) shouldBe an [IntColumn]
      mkCol(1L, 2L) shouldBe a [LongColumn]
      mkCol(1D, 2D) shouldBe a [DoubleColumn]
    }

    "force manual spec through map" in {
      val col = Column.dense(Array("1","2","3"), Mask(1))
      col.map(_.toDouble) shouldBe a [DoubleColumn]
      col.map(_.toInt) shouldBe an [IntColumn]
      col.map(_.toLong) shouldBe a [LongColumn]
    }

    "force manual spec through flatMap" in {
      val col = Column.dense(Array("1","2","3"), Mask(1))
      col.flatMap(n => Value(n.toDouble)) shouldBe a [DoubleColumn]
      col.flatMap(n => Value(n.toInt)) shouldBe an [IntColumn]
      col.flatMap(n => Value(n.toLong)) shouldBe a [LongColumn]
    }

    "retain manual spec through filter" in {
      Column.dense(Array(1,2,3)).filter(_ == 2) shouldBe an [IntColumn]
      Column.dense(Array(1D,2D,3D)).filter(_ == 2) shouldBe a [DoubleColumn]
      Column.dense(Array(1L,2L,3L)).filter(_ == 2) shouldBe a [LongColumn]
    }

    "retain manual spec through orElse" in {
      Column(Value(1)).orElse(Column(Value(2))) shouldBe an [IntColumn]
      Column(Value(1L)).orElse(Column(Value(2L))) shouldBe a [LongColumn]
      Column(Value(1D)).orElse(Column(Value(2D))) shouldBe a [DoubleColumn]
    }

    "retain manual spec through reindex" in {
      Column.dense(Array(1,2,3)).reindex(Array(2,1,0)) shouldBe an [IntColumn]
      Column.dense(Array(1D,2D,3D)).reindex(Array(2,1,0)) shouldBe a [DoubleColumn]
      Column.dense(Array(1L,2L,3L)).reindex(Array(2,1,0)) shouldBe a [LongColumn]
    }

    "retain manual spec through force" in {
      Column.dense(Array(1,2,3)).force(2) shouldBe an [IntColumn]
      Column.dense(Array(1D,2D,3D)).force(2) shouldBe a [DoubleColumn]
      Column.dense(Array(1L,2L,3L)).force(2) shouldBe a [LongColumn]
    }

    "retain manual spec through mask" in {
      val mask = Mask(1)
      Column.dense(Array(1,2,3)).mask(mask) shouldBe an [IntColumn]
      Column.dense(Array(1D,2D,3D)).mask(mask) shouldBe a [DoubleColumn]
      Column.dense(Array(1L,2L,3L)).mask(mask) shouldBe a [LongColumn]
    }

    "retain manual spec through setNA" in {
      Column.dense(Array(1,2,3)).setNA(Int.MinValue) shouldBe an [IntColumn]
      Column.dense(Array(1D,2D,3D)).setNA(Int.MinValue) shouldBe a [DoubleColumn]
      Column.dense(Array(1L,2L,3L)).setNA(Int.MinValue) shouldBe a [LongColumn]
    }

    "setNA should be no-op when row already NA" in {
      val col = Column.dense(Array(1,2,3)).asInstanceOf[IntColumn]
      Column.dense(Array(1,2,3)).setNA(Int.MinValue).setNA(3).setNA(Int.MaxValue).asInstanceOf[IntColumn].naValues.max should === (None)
      Column.dense(Array(1L,2L,3L)).setNA(Int.MinValue).setNA(3).setNA(Int.MaxValue).asInstanceOf[LongColumn].naValues.max should === (None)
      Column.dense(Array(1D,2D,3D)).setNA(Int.MinValue).setNA(3).setNA(Int.MaxValue).asInstanceOf[DoubleColumn].naValues.max should === (None)
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
      Column.eval(Value(_)).reindex(Array(1,3,2)) shouldBe a [DenseColumn[_]]
    }

    "return dense columns from force" in {
      Column.eval(Value(_)).force(5) shouldBe a [DenseColumn[_]]
    }

    "not overflow index on shift" in {
      Column.eval(Value(_)).shift(1)(Int.MinValue) should === (NA)
    }
  }
}
