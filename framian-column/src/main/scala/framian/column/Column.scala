package framian
package column

import scala.language.experimental.macros

import scala.reflect.macros.blackbox

import scala.collection.immutable.BitSet

sealed trait Column[+A] extends (Int => Cell[A]) {
  def foldRow[B](row: Int)(na: B, nm: B, f: A => B): B = macro ColumnMacros.foldRowImpl[A, B]

  def cellMap[B](f: Cell[A] => Cell[B]): Column[B]

  def map[B](f: A => B): Column[B]
  def filter(p: A => Boolean): Column[A]

  override def toString: String =
    (0 to 5).map(apply(_).toString).mkString("Column(", ", ", ")")
}

sealed trait BoxedColumn[A] extends Column[A] {
  def map[B](f: A => B): Column[B] = cellMap {
    case Value(a) => Value(f(a))
    case (nonValue: NonValue) => nonValue
  }

  def filter(p: A => Boolean): Column[A] = cellMap {
    case Value(a) if p(a) => Value(a)
    case Value(_) => NA
    case nonValue => nonValue
  }
}

sealed trait UnboxedColumn[A] extends Column[A] {
  def isValueAt(row: Int): Boolean
  def nonValueAt(row: Int): NonValue
  def valueAt(row: Int): A

  def apply(row: Int): Cell[A] =
    if (isValueAt(row)) Value(valueAt(row))
    else nonValueAt(row)
}

object Column {
  def eval[A](get: Int => Cell[A]): Column[A] = EvalColumn(get)
  def dense[A](values: Array[A]): Column[A] = values match {
    case (values: Array[Double]) => DoubleColumn(values, BitSet.empty, BitSet.empty)
    case (values: Array[Int]) => IntColumn(values, BitSet.empty, BitSet.empty)
    case (values: Array[Long]) => LongColumn(values, BitSet.empty, BitSet.empty)
    case _ => GenColumn[A](values, BitSet.empty, BitSet.empty)
  }
}

// Wraps Int => Cell[A].
case class EvalColumn[A](f: Int => Cell[A]) extends BoxedColumn[A] {
  override def apply(row: Int): Cell[A] = f(row)

  def cellMap[B](g: Cell[A] => Cell[B]): Column[B] = EvalColumn(f andThen g)
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

// Array-backed instances.
// sealed trait DenseColumn[@spec(Int, Long, Double) A] {
//   def values: Array[A]
//   def naValues: BitSet
//   def nmValues: BitSet
// }
// 
// case class DoubleColumn(values: Array[Double], naValues: BitSet, nmValues: BitSet) extends FiniteColumn[Double]

sealed trait EmptyColumn[A] extends BoxedColumn[A] {
  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = EvalColumn(this andThen f)
}

case class NAColumn[A](nmValues: BitSet) extends EmptyColumn[A] {
  def apply(row: Int): Cell[A] = if (nmValues(row)) NM else NA
}

case class NMColumn[A](naValues: BitSet) extends EmptyColumn[A] {
  def apply(row: Int): Cell[A] = if (naValues(row)) NA else NM
}

sealed trait DenseColumn[A] extends UnboxedColumn[A] {
  def values: Array[_]
  def naValues: BitSet
  def nmValues: BitSet

  private final def valid(row: Int) = row >= 0 && row < values.length
  def isValueAt(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def nonValueAt(row: Int): NonValue = if (nmValues(row)) NM else NA

  def filter(p: A => Boolean): Column[A] = ???

  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = ???
}

object DenseColumn extends DenseColumnFunctions

case class IntColumn(values: Array[Int], naValues: BitSet, nmValues: BitSet) extends DenseColumn[Int] {
  def valueAt(row: Int): Int = values(row)
  def map[B](f: Int => B): Column[B] = DenseColumn.mapInt(values, naValues, nmValues, f)
}

case class LongColumn(values: Array[Long], naValues: BitSet, nmValues: BitSet) extends DenseColumn[Long] {
  def valueAt(row: Int): Long = values(row)
  def map[B](f: Long => B): Column[B] = DenseColumn.mapLong(values, naValues, nmValues, f)
}

case class DoubleColumn(values: Array[Double], naValues: BitSet, nmValues: BitSet) extends DenseColumn[Double] {
  def valueAt(row: Int): Double = values(row)
  def map[B](f: Double => B): Column[B] = DenseColumn.mapDouble(values, naValues, nmValues, f)
}

case class AnyColumn[A](values: Array[Any], naValues: BitSet, nmValues: BitSet) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row).asInstanceOf[A]
  def map[B](f: A => B): Column[B] = DenseColumn.mapAny(values, naValues, nmValues, f)
}

case class GenColumn[A](values: Array[A], naValues: BitSet, nmValues: BitSet) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row)
  def map[B](f: A => B): Column[B] = DenseColumn.mapGeneric(values, naValues, nmValues, f)
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
            case NA => $na
            case NM => $nm
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
