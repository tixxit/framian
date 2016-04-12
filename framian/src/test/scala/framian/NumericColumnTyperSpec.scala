package framian

import scala.reflect.{ ClassTag, classTag }

import spire.math.{ Rational, Number, ConvertableTo }
import spire.algebra._
import spire.std.any._
import spire.syntax.field._
import spire.syntax.literals._

import shapeless._

class NumericColumnTyperSpec extends FramianSpec {
  val MinInt = Int.MinValue
  val MaxInt = Int.MaxValue
  val MinLong = Long.MinValue
  val MaxLong = Long.MaxValue

  implicit class ColumnOps[A](col: Column[A]) {
    def cells(rows: Seq[Int]): Vector[Cell[A]] =
      rows.map(col(_))(collection.breakOut)
  }

  def untyped[A: ClassTag](xs: A*): UntypedColumn = TypedColumn(Column.dense(xs.toArray))

  def checkCast[A: ClassTag, B: ColumnTyper: ClassTag](casts: (A, Cell[B])*) = {
    val (input, output) = casts.unzip
    val col = untyped(input: _*).cast[B]

    (0 until input.size).map(col(_)) should === (output)
  }

  "NumericColumnTyper" should {
    "cast Int to anything" in {
      val values = Vector(1, 2, MinInt, MaxInt, 0)
      val col = untyped(values: _*)
      col.cast[Int].cells(0 to 4) should === (values map (Value(_)))
      col.cast[Long].cells(0 to 4) should === (values map (n => Value(n.toLong)))
      col.cast[Float].cells(0 to 4) should === (values map (n => Value(n.toFloat)))
      col.cast[Double].cells(0 to 4) should === (values map (n => Value(n.toDouble)))
      col.cast[BigInt].cells(0 to 4) should === (values map (n => Value(BigInt(n))))
      col.cast[BigDecimal].cells(0 to 4) should === (values map (n => Value(BigDecimal(n))))
      col.cast[Rational].cells(0 to 4) should === (values map (n => Value(Rational(n))))
      col.cast[Number].cells(0 to 4) should === (values map (n => Value(Number(n))))
    }
  }

  "big Long cast to Int is not meaningful" in
    checkCast[Long, Int]((MaxInt.toLong + 1) -> NM, MinLong -> NM, MaxLong -> NM)

  "small Long cast to Int is valid" in
    checkCast[Long, Int](MaxInt.toLong -> Value(MaxInt), 0L -> Value(0), -100L -> Value(-100))

  "checking bounded Int" should checkBoundedInteger[Int](MinInt, MaxInt)

  "Int cast to Long is always valid" in
    checkCast[Int, Long](MaxInt -> Value(MaxInt.toLong), 0 -> Value(0L), -100 -> Value(-100L))

  "checking bounded Long" should checkBoundedInteger[Long](MinLong, MaxLong)

  "Int cast to BigInt is always valid" in
    checkCast[Int, BigInt](MaxInt -> Value(BigInt(MaxInt)), 0 -> Value(BigInt(0)), -100 -> Value(BigInt(-100)))

  "Long cast to BigInt is always valid" in
    checkCast[Long, BigInt](MaxLong -> Value(BigInt(MaxLong)), 0L -> Value(BigInt(0)), -100L -> Value(BigInt(-100)))

  testFractional[Float]
  testFractional[Double]
  testFractional[BigDecimal]
  testFractional[Rational]
  testFractional[Number]

  def checkBoundedInteger[A: ClassTag: EuclideanRing: ColumnTyper](min: A, max: A) = {
    val nameA = classTag[A].runtimeClass.getSimpleName

    def checkFractional[B: ClassTag: Ring](fromStr: String => B) = {
      val nameB = classTag[B].runtimeClass.getSimpleName

      s"small, whole $nameB cast to $nameA is valid" in
        checkCast[B, A](fromStr("100") -> Value(Ring[A].fromInt(100)), fromStr(min.toString) -> Value(min))

      s"fractional $nameB cast to $nameA is not meaningful" in
        checkCast[B, A](fromStr("0.1") -> NM, fromStr("100.5") -> NM)

      s"large $nameB cast to $nameA is not meaningful" in
        checkCast[B, A](2 * fromStr(min.toString) -> NM, 2 * fromStr(max.toString) -> NM)
    }

    "small BigInt cast to $nameA is valid" in checkCast[BigInt, A](
      BigInt(min.toString) -> Value(min),
      BigInt(max.toString) -> Value(max),
      BigInt(0) -> Value(Ring[A].zero),
      BigInt(1) -> Value(Ring[A].one))

    "big BigInt cast to $nameA is not meaningful" in
      checkCast[BigInt, A]((BigInt(min.toString) - 1) -> NM, (BigInt(max.toString) + 1) -> NM)

    checkFractional[Float](_.toFloat)
    checkFractional[Double](_.toDouble)
    checkFractional[BigDecimal](BigDecimal(_))
    checkFractional[Rational](Rational(_))
    checkFractional[Number](Number(_))

    "cast anything to $nameA" in
      checkCast[Any, A](
        0 -> Value(Ring[A].fromInt(0)),
        1L -> Value(Ring[A].fromInt(1)),
        2F -> Value(Ring[A].fromInt(2)),
        3D -> Value(Ring[A].fromInt(3)),
        BigInt(4) -> Value(Ring[A].fromInt(4)),
        BigDecimal(5) -> Value(Ring[A].fromInt(5)),
        Rational(6) -> Value(Ring[A].fromInt(6)),
        Number(7) -> Value(Ring[A].fromInt(7)))
  }

  def testFractional[A: ClassTag: Field: ConvertableTo: ColumnTyper] = {
    val nameA = classTag[A].runtimeClass.getSimpleName
    val convertable = ConvertableTo[A]
    import convertable._

    s"cast anything to $nameA" in 
      checkCast[Any, A](
        MaxInt -> Value(fromInt(MaxInt)),
        MinLong -> Value(fromLong(MinLong)),
        BigInt(-100) -> Value(fromInt(-100)),
        BigDecimal("0.1") -> Value(fromBigDecimal(BigDecimal("0.1"))),
        Rational(1, 2) -> Value(fromDouble(0.5)),
        Number(1.625) -> Value(fromDouble(1.625))
      )

    s"cast Int to $nameA" in
      checkCast[Int, A](MinInt -> Value(fromInt(MinInt)), MaxInt -> Value(fromInt(MaxInt)), 0 -> Value(fromInt(0)), -10 -> Value(fromInt(-10)))

    s"cast Long to $nameA" in
      checkCast[Long, A](MinLong -> Value(fromLong(MinLong)), MaxLong -> Value(fromLong(MaxLong)), 0L -> Value(fromInt(0)), -10L -> Value(fromInt(-10)))

    s"cast BigInt to $nameA" in
      checkCast[BigInt, A](BigInt(0) -> Value(fromInt(0)), BigInt("-1000000000000") -> Value(fromBigInt(BigInt("-1000000000000"))))

    s"cast Float to $nameA" in
      checkCast[Float, A](0.1F -> Value(fromFloat(0.1F)), -1e32F -> Value(fromFloat(-1e32F)))

    s"cast Double to $nameA" in
      checkCast[Double, A](-0.25 -> Value(fromDouble(-0.25)), 1e100 -> Value(fromDouble(1e100)))

    s"cast BigDecimal to $nameA" in
      checkCast[BigDecimal, A](BigDecimal(0) -> Value(fromInt(0)), BigDecimal("12345.789") -> Value(fromBigDecimal(BigDecimal("12345.789"))))

    s"cast Rational to $nameA" in
      checkCast[Rational, A](r"1/3" -> Value(fromRational(r"1/3")), r"1234/1235" -> Value(fromRational(r"1234/1235")))

    s"cast Number to $nameA" in
      checkCast[Number, A](
        Number(0) -> Value(fromInt(0)),
        Number(BigInt("1000000000001")) -> Value(fromBigInt(BigInt("1000000000001"))),
        Number(BigDecimal("1.234")) -> Value(fromBigDecimal(BigDecimal("1.234")))
        //Number(r"1/3") -> Value(fromRational(r"1/3")) // fromRational is broken for Number in Spire.
      )
  }
}
