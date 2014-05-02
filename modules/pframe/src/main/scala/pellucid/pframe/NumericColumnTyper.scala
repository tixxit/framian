package pellucid.pframe

import scala.math.ScalaNumericAnyConversions
import scala.reflect.{ ClassTag, classTag }
import scala.{ specialized => spec }
import scala.util.Try

import spire.math.{ Rational, Number }

import shapeless._

// Currently support the following number types:
//   - Int
//   - Long
//   - Float
//   - Double
//   - BigInt
//   - BigDecimal
//   - Rational
//   - Number
//
// TODO: Support Algebraic and Real.

trait NumericColumnTyper[@spec(Int,Long,Float,Double) A] extends ColumnTyper[A] {
  def castValue(x: Any): Option[A]
  def castColumn(column: TypedColumn[_]): Option[Column[A]]

  def cast(col: TypedColumn[_]): Column[A] = {
    val column: Column[Any] = col.column
    castColumn(col) getOrElse Column.wrap { row =>
      column(row) flatMap { x =>
        Cell.fromOption(castValue(x), NM)
      }
    }
  }
}

private object Classes {
  val Byte = java.lang.Byte.TYPE
  val Short = java.lang.Short.TYPE
  val Int = java.lang.Integer.TYPE
  val Long = java.lang.Long.TYPE
  val Float = java.lang.Float.TYPE
  val Double = java.lang.Double.TYPE
  val BigInt = classOf[scala.math.BigInt]
  val BigInteger = classOf[java.math.BigInteger]
  val BigDecimal = classOf[scala.math.BigDecimal]
  val JavaBigDecimal = classOf[java.math.BigDecimal]
  val Rational = classOf[Rational]
  val Number = classOf[Number]
  val String = classOf[String]
}

object NumericColumnTyper {
  type ScalaNum = ScalaNumericAnyConversions

  private val scalaNumberClass = classTag[ScalaNumericAnyConversions].runtimeClass

  def isScalaNumber(runtimeClass: Class[_]): Boolean =
    scalaNumberClass isAssignableFrom runtimeClass

  def convert[A, B](source: Column[A])(isValid: A => Boolean, to: A => B): Column[B] =
    new Column[B] {
      def exists(row: Int): Boolean = source.exists(row) && isValid(source.value(row))
      def missing(row: Int): Missing = if (source.exists(row)) NM else source.missing(row)
      def value(row: Int): B = to(source.value(row))
    }

  def foldValue[A](x: Any)(
      primInt: Long => A,
      primFloat: Double => A,
      bigInt: BigInt => A,
      bigFloat: BigDecimal => A,
      rational: Rational => A,
      string: String => A,
      z: => A): A = {
    x match {
      case (x: Byte) => primInt(x.asInstanceOf[Byte].toLong)
      case (x: Short) => primInt(x.toLong)
      case (x: Int) => primInt(x.toLong)
      case (x: Long) => primInt(x)
      case (x: Float) => primFloat(x.toDouble)
      case (x: Double) => primFloat(x)
      case (x: BigInt) => bigInt(x)
      case (x: java.math.BigInteger) => bigInt(BigInt(x))
      case (x: BigDecimal) => bigFloat(x)
      case (x: java.math.BigDecimal) => bigFloat(BigDecimal(x))
      case (x: Rational) => rational(x)
      case (x: Number) =>
        if (x.isWhole) {
          if (x.withinLong) primInt(x.toLong)
          else bigInt(x.toBigInt)
        } else {
          if (x.isExact) rational(x.toRational)
          else bigFloat(x.toBigDecimal)
        }
      case (x: String) => string(x)
      case _ => z
    }
  }

  def foldColumn[A](col: TypedColumn[_])(
      primInt: Column[Long] => A,
      primFloat: Column[Double] => A,
      bigInt: Column[BigInt] => A,
      bigFloat: Column[BigDecimal] => A,
      rational: Column[Rational] => A,
      string: Column[String] => A,
      z: => A): A = {
    val column = col.column
    val runtimeClass = col.classTagA.runtimeClass
    runtimeClass match {
      case Classes.Byte => primInt(column.asInstanceOf[Column[Byte]] map (_.toLong))
      case Classes.Short => primInt(column.asInstanceOf[Column[Short]] map (_.toLong))
      case Classes.Int => primInt(column.asInstanceOf[Column[Int]] map (_.toLong))
      case Classes.Long => primInt(column.asInstanceOf[Column[Long]])
      case Classes.Float => primFloat(column.asInstanceOf[Column[Float]] map (_.toDouble))
      case Classes.Double => primFloat(column.asInstanceOf[Column[Double]])
      case Classes.BigInt => bigInt(column.asInstanceOf[Column[BigInt]])
      case Classes.BigInteger => bigInt(column.asInstanceOf[Column[java.math.BigInteger]] map (BigInt(_)))
      case Classes.BigDecimal => bigFloat(column.asInstanceOf[Column[BigDecimal]])
      case Classes.JavaBigDecimal => bigFloat(column.asInstanceOf[Column[java.math.BigDecimal]] map (BigDecimal(_)))
      case cls if Classes.Rational isAssignableFrom cls => rational(column.asInstanceOf[Column[Rational]])
      case Classes.String => string(column.asInstanceOf[Column[String]])
      case _ => z
    }
  }
}

import NumericColumnTyper._

final class IntColumnTyper extends ColumnTyper[Int] {
  private def safeToInt[A](n: A)(implicit f: A => ScalaNumericAnyConversions): Cell[Int] = {
    val m = f(n).toInt
    if (n == m) Value(m) else NM
  }

  private val castValue: Any => Cell[Int] = foldValue(_)(
    n => if (n >= Int.MinValue && n <= Int.MaxValue) Value(n.toInt) else NM,
    safeToInt(_),
    n => if (n >= Int.MinValue && n <= Int.MaxValue) Value(n.toInt) else NM,
    safeToInt(_),
    safeToInt(_),
    n => Try(BigDecimal(n).toInt).toOption.fold[Cell[Int]](NM) { Value(_) },
    NM
  )

  def cast(col: TypedColumn[_]): Column[Int] = {
    val column = col.column
    val runtimeClass = col.classTagA.runtimeClass
    runtimeClass match {
      case Classes.Byte => column.asInstanceOf[Column[Byte]] map (_.toInt)
      case Classes.Short => column.asInstanceOf[Column[Short]] map (_.toInt)
      case Classes.Int => column.asInstanceOf[Column[Int]]
      case _ => Column.wrap(column(_) flatMap castValue)
    }
  }
}

final class LongColumnTyper extends NumericColumnTyper[Long] {
  private def safeToLong[A](n: A)(implicit f: A => ScalaNumericAnyConversions): Option[Long] = {
    val m = f(n).toLong
    if (n == m) Some(m) else None
  }

  def castValue(x: Any): Option[Long] =
    foldValue(x)(
      Some(_),
      safeToLong(_),
      safeToLong(_),
      safeToLong(_),
      { n =>
        if (n.isWhole) {
          val n0 = n.numerator
          val m = n0.toLong
          if (m == n0) Some(m) else None
        } else None
      },
      n => Try(java.lang.Long.parseLong(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Long]] =
    foldColumn(col)(
      col => Some(col),
      col => None,
      col => None,
      col => None,
      col => None,
      col => None, // TODO: why are these all None?
      None
    )
}

final class FloatColumnTyper extends ColumnTyper[Float] {
  private val doubleTyper = new DoubleColumnTyper
  def cast(col: TypedColumn[_]): Column[Float] =
    doubleTyper.cast(col) map (_.toFloat)
}

final class DoubleColumnTyper extends NumericColumnTyper[Double] {
  def castValue(x: Any): Option[Double] =
    foldValue(x)(
      n => Some(n.toDouble),
      n => Some(n),
      n => Some(n.toDouble),
      n => Some(n.toDouble),
      n => Some(n.toDouble),
      n => Try(java.lang.Double.parseDouble(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Double]] =
    foldColumn(col)(
      col => Some(col map (_.toDouble)),
      col => Some(col),
      col => Some(col map (_.toDouble)),
      col => Some(col map (_.toDouble)),
      col => Some(col map (_.toDouble)),
      col => Some(col map (java.lang.Double.parseDouble(_))),
      None
    )
}

final class BigIntTyper extends NumericColumnTyper[BigInt] {
  def castValue(x: Any): Option[BigInt] =
    foldValue(x)(
      n => Some(BigInt(n)),
      n => if (n.isWhole) Some(BigDecimal(n).toBigInt) else None,
      n => Some(n),
      n => if (n.isWhole) Some(n.toBigInt) else None,
      n => if (n.isWhole) Some(n.numerator) else None,
      n => Try(BigInt(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[BigInt]] =
    foldColumn(col)(
      col => Some(col map (BigInt(_))),
      col => None,
      col => Some(col),
      col => None,
      col => None,
      col => None,
      None
    )
}

final class BigDecimalTyper extends NumericColumnTyper[BigDecimal] {
  def castValue(x: Any): Option[BigDecimal] =
    foldValue(x)(
      n => Some(BigDecimal(n)),
      n => Some(BigDecimal(n)),
      n => Some(BigDecimal(n)),
      n => Some(n),
      n => Try(n.toBigDecimal).toOption,
      n => Try(BigDecimal(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[BigDecimal]] =
    foldColumn(col)(
      col => Some(col map (BigDecimal(_))),
      col => Some(col map (BigDecimal(_))),
      col => Some(col map (BigDecimal(_))),
      col => Some(col),
      col => None,
      col => None,
      None
    )
}

final class RationalTyper extends NumericColumnTyper[Rational] {
  def castValue(x: Any): Option[Rational] =
    foldValue(x)(
      n => Some(Rational(n)),
      n => Some(Rational(n)),
      n => Some(Rational(n)),
      n => Some(Rational(n)),
      n => Some(n),
      n => Try(Rational(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Rational]] =
    foldColumn(col)(
      col => Some(col map (Rational(_))),
      col => Some(col map (Rational(_))),
      col => Some(col map (Rational(_))),
      col => Some(col map (Rational(_))),
      col => Some(col),
      col => Some(col map (Rational(_))),
      None
    )
}

final class NumberTyper extends NumericColumnTyper[Number] {
  def castValue(x: Any): Option[Number] =
    foldValue(x)(
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Try(Number(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Number]] =
    foldColumn(col)(
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      None
    )
}
