package framian
package column

import scala.language.experimental.macros

import scala.{specialized => sp }
import scala.annotation.unspecialized

import scala.reflect.macros.blackbox

sealed trait Column[@sp(Int,Long,Double) +A] {

  @unspecialized
  def foldRow[B](row: Int)(na: B, nm: B, f: A => B): B = macro ColumnMacros.foldRowImpl[A, B]

  def apply(row: Int): Cell[A]

  def cellMap[B](f: Cell[A] => Cell[B]): Column[B]

  def map[@sp(Int,Long,Double) B](f: A => B): Column[B]

  def filter(p: A => Boolean): Column[A]

  def reindex(index: Array[Int]): Column[A]

  def force(len: Int): Column[A]

  def mask(na: Mask): Column[A]

  def setNA(row: Int): Column[A] = mask(Mask(row)) // TODO: Implement with Mask#+?

  override def toString: String =
    (0 to 5).map(apply(_).toString).mkString("Column(", ", ", ")")
}

sealed trait BoxedColumn[A] extends Column[A] {
  def map[@sp(Int,Long,Double) B](f: A => B): Column[B] = cellMap {
    case Value(a) => Value(f(a))
    case (nonValue: NonValue) => nonValue
  }

  def filter(p: A => Boolean): Column[A] = cellMap {
    case Value(a) if p(a) => Value(a)
    case Value(_) => NA
    case nonValue => nonValue
  }
}

sealed trait UnboxedColumn[@sp(Int,Long,Double) A] extends Column[A] {
  def isValueAt(row: Int): Boolean
  def nonValueAt(row: Int): NonValue
  def valueAt(row: Int): A

  def apply(row: Int): Cell[A] =
    if (isValueAt(row)) Value(valueAt(row))
    else nonValueAt(row)
}

object Column {
  def eval[A](get: Int => Cell[A]): Column[A] = EvalColumn(get)
  def dense[A](values: Array[A], na: Mask = Mask.empty, nm: Mask = Mask.empty): Column[A] = values match {
    case (values: Array[Double]) => DoubleColumn(values, na, nm)
    case (values: Array[Int]) => IntColumn(values, na, nm)
    case (values: Array[Long]) => LongColumn(values, na, nm)
    case _ => GenericColumn[A](values, na, nm)
  }
}

// Wraps Int => Cell[A].
case class EvalColumn[A](f: Int => Cell[A]) extends BoxedColumn[A] {
  override def apply(row: Int): Cell[A] = f(row)

  def cellMap[B](g: Cell[A] => Cell[B]): Column[B] = EvalColumn(f andThen g)

  def reindex(index: Array[Int]): Column[A] =
    DenseColumn.force(index andThen f, index.length)

  def force(len: Int): Column[A] =
    DenseColumn.force(f, len)

  def mask(mask: Mask): Column[A] = EvalColumn { row =>
    if (mask(row)) NA else f(row)
  }
}

// Wraps a column and memoizes the results.
// case class MemoizingColumn[A](col: Column[A]) extends Column[A] {
//   private val cache: Map[Int, Cell[A]] = _
// 
//   override def apply(row: Int): Cell[A] = {
//     val cached = cache.get(row)
//     if (cached == null) {
//       val result = col(row)
//       val sneaky = cache.putIfAbsent(result)
//       if (sneaky != null) sneaky else result
//     } else {
//       cached
//     }
//   }
// }

sealed trait EmptyColumn[A] extends BoxedColumn[A] {
  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = EvalColumn((apply _) andThen f)
  def reindex(index: Array[Int]): Column[A] = {
    val nm = Mask.newBuilder
    var i = 0
    while (i < index.length) {
      if (apply(index(i)) == NM)
        nm += i
      i += 1
    }
    NAColumn(nm.result())
  }

  def force(len: Int): Column[A] = this match {
    case NAColumn(nm) =>
      NAColumn(nm.filter(row => row >= 0 && row < len))
    case NMColumn(na) =>
      val nm = Mask.newBuilder
      (0 until len) foreach { i =>
        if (!na(i)) nm += i
      }
      NAColumn(nm.result())
  }
}

case class NAColumn[A](nmValues: Mask) extends EmptyColumn[A] {
  def apply(row: Int): Cell[A] = if (nmValues(row)) NM else NA
  def mask(mask: Mask): Column[A] = NAColumn(nmValues -- mask)
}

case class NMColumn[A](naValues: Mask) extends EmptyColumn[A] {
  def apply(row: Int): Cell[A] = if (naValues(row)) NA else NM
  def mask(mask: Mask): Column[A] = NAColumn(naValues | mask)
}

sealed trait DenseColumn[@sp(Int,Long,Double) A] extends UnboxedColumn[A] {
  def values: Array[_]
  def naValues: Mask
  def nmValues: Mask

  private final def valid(row: Int) = row >= 0 && row < values.length
  def isValueAt(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def nonValueAt(row: Int): NonValue = if (nmValues(row)) NM else NA

  def filter(p: A => Boolean): Column[A] = {
    val na = Mask.newBuilder
    var i = 0
    while (i < values.length) {
      if (naValues(i) || (isValueAt(i) && !p(valueAt(i)))) {
        na += i
      }
      i += 1
    }
    Column.dense(values, na.result(), nmValues).asInstanceOf[Column[A]]
  }

  def mask(na: Mask): Column[A] =
    Column.dense(values, naValues | na, nmValues -- na).asInstanceOf[Column[A]]

  // Required, because cellmap can make an infinite column.
  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = Column.eval(apply _).cellMap(f)
}

object DenseColumn extends DenseColumnFunctions

case class IntColumn(values: Array[Int], naValues: Mask, nmValues: Mask) extends DenseColumn[Int] {
  def valueAt(row: Int): Int = values(row)
  def map[@sp(Int,Long,Double) B](f: Int => B): Column[B] = DenseColumn.mapInt(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Int] = DenseColumn.reindexInt(index, values, naValues, nmValues)
  def force(len: Int): Column[Int] = IntColumn(
    java.util.Arrays.copyOf(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

case class LongColumn(values: Array[Long], naValues: Mask, nmValues: Mask) extends DenseColumn[Long] {
  def valueAt(row: Int): Long = values(row)
  def map[@sp(Int,Long,Double) B](f: Long => B): Column[B] = DenseColumn.mapLong(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Long] = DenseColumn.reindexLong(index, values, naValues, nmValues)
  def force(len: Int): Column[Long] = LongColumn(
    java.util.Arrays.copyOf(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

case class DoubleColumn(values: Array[Double], naValues: Mask, nmValues: Mask) extends DenseColumn[Double] {
  def valueAt(row: Int): Double = values(row)
  def map[@sp(Int,Long,Double) B](f: Double => B): Column[B] = DenseColumn.mapDouble(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Double] = DenseColumn.reindexDouble(index, values, naValues, nmValues)
  def force(len: Int): Column[Double] = DoubleColumn(
    java.util.Arrays.copyOf(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

case class AnyColumn[A](values: Array[Any], naValues: Mask, nmValues: Mask) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row).asInstanceOf[A]
  def map[@sp(Int,Long,Double) B](f: A => B): Column[B] = DenseColumn.mapAny(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[A] = DenseColumn.reindexAny(index, values, naValues, nmValues)
  def force(len: Int): Column[A] = AnyColumn(
    DenseColumn.copyArray(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

case class GenericColumn[A](values: Array[A], naValues: Mask, nmValues: Mask) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row)
  def map[@sp(Int,Long,Double) B](f: A => B): Column[B] = DenseColumn.mapGeneric(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[A] = DenseColumn.reindexGeneric(index, values, naValues, nmValues)
  def force(len: Int): Column[A] = GenericColumn(
    DenseColumn.copyArray(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

class ColumnMacros(val c: blackbox.Context) {
  import c.universe._

  def foldRowImpl[A, B](row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Tree = {
    val cell = newTermName(c.fresh("foldRow$cell$"))
    val col = newTermName(c.fresh("foldRow$col$"))
    val value = newTermName(c.fresh("foldRow$value$"))
    val r = newTermName(c.fresh("foldRow$row$"))

    val tree = q"""
    ${c.prefix} match {
      case ($col: UnboxedColumn[_]) =>
        val $r = $row
        if ($col.isValueAt($r)) {
          $f($col.valueAt($r))
        } else {
          $col.nonValueAt($r) match {
            case _root_.framian.NA => $na
            case _root_.framian.NM => $nm
          }
        }

      case $col =>
        $col($row) match {
          case _root_.framian.NA => $na
          case _root_.framian.NM => $nm
          case _root_.framian.Value($value) => $f($value)
        }
    }
    """

    c.resetLocalAttrs(tree)
  }
}
