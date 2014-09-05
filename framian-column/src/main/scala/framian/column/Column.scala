package framian
package column

import java.util.concurrent.ConcurrentHashMap

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

  // def orElse(that: Column[A]): Column[A]

  /**
   * Returns a column whose `i`-th row maps to row `index(i)` in this column.
   * If `i &lt; 0` or `i &gt;= index.length` then the returned column returns
   * [[NA]]. This always forces all rows in `index` and the returned column is
   * *dense* and unboxed.
   */
  def reindex(index: Array[Int]): Column[A]

  /**
   * Returns a column which has had all rows between `0` and `len` (exclusive)
   * forced (evaluated) and stored in memory, while all rows outside of `0` and
   * `len` are set to [[NA]]. The returned column is *dense* and unboxed.
   *
   * @param len the upper bound of the range of values to force
   */
  def force(len: Int): Column[A]

  /**
   * Returns a column with rows contained in `na` masked to [[NA]]s.
   *
   * @param na the rows to mask in the column
   */
  def mask(na: Mask): Column[A]

  /**
   * Returns a column with a single row forced to [[NA]] and all others
   * remaining the same. This is equivalent to, but possibly more efficient
   * than `col.mask(Mask(row))`.
   *
   * @param row the row that will be forced to [[NA]]
   */
  def setNA(row: Int): Column[A]

  /**
   * Returns a copy of this column whose values will be memoized if they are
   * evaluated. That is, if `this` column is an *eval* column, then memoizing
   * it will ensure that, for each row, the value is only computed once,
   * regardless of the number of times it is accessed.
   *
   * By default, the memoization is always pessimistic (guaranteed at-most-once
   * evaluation). If `optimistic` is `true`, then the memoizing may use an
   * optimistic update strategy, which means a value *may* be evaluated more
   * than once if it accessed concurrently.
   *
   * For dense, empty, and previously-memoized columns, this just returns the
   * column itself.
   *
   * @param optimistic if true, memoized column may use optimistic updates
   */
  def memoize(optimistic: Boolean = false): Column[A]

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
  final def newBuilder[A: GenColumnBuilder](): ColumnBuilder[A] = ColumnBuilder[A]()

  /**
   * Returns a column which defaults to [[NA]] for all rows, except those in
   * `nm`, which are [[NM]].
   */
  def empty[A](nm: Mask = Mask.empty): Column[A] = if (nm.isEmpty) {
    new EmptyColumn[A]
  } else {
    new AnyColumn[A](new Array[Any](0), Mask.empty, nm)
  }

  /**
   * Returns a column which returns `Value(a)` for all rows.
   *
   * @note The `value` argument is strict.
   */
  def value[A](value: A): Column[A] = {
    val cell = Value(value)
    EvalColumn(_ => cell)
  }

  /**
   * Returns a column whose values are obtained using `get`. Each time a row is
   * accessed, `get` will be re-evaluated. To ensure values are evaluated
   * only once, you can [[memoize]] the column or use on of the *forcing*
   * methods, such as [[reindex]] or [[force]].
   */
  def eval[A](get: Int => Cell[A]): Column[A] = EvalColumn(get)

  /**
   * Create a dense column from an array of values. A dense column can still
   * have empty values, [[NA]] and [[NM]], as specified with the `na` and `nm`
   * masks respectively. Dense columns are unboxed and only values that aren't
   * masked by `na` and `nm` will ever be returned (so they can be `null`,
   * `NaN`, etc.)
   *
   * The [[NM]] mask (`nm`) always takes precedence over the [[NA]] mask
   * (`na`).  If a row is outside of the range 0 until `values.length`, then if
   * `nm(row)` is true, [[NM]] will be returned, otherwise [[NA]] is returned.
   *
   * @param values the values of the column, rows correspond to indices
   * @param na     masked rows that will return [[NA]]
   * @param nm     masked rows that will return [[NM]]
   */
  def dense[A](values: Array[A], na: Mask = Mask.empty, nm: Mask = Mask.empty): Column[A] = values match {
    case (values: Array[Double]) => DoubleColumn(values, na, nm)
    case (values: Array[Int]) => IntColumn(values, na, nm)
    case (values: Array[Long]) => LongColumn(values, na, nm)
    case _ => GenericColumn[A](values, na, nm)
  }
}

private[column] case class EvalColumn[A](f: Int => Cell[A]) extends BoxedColumn[A] {
  override def apply(row: Int): Cell[A] = f(row)

  def cellMap[B](g: Cell[A] => Cell[B]): Column[B] = EvalColumn(f andThen g)

  def reindex(index: Array[Int]): Column[A] =
    DenseColumn.force(index andThen f, index.length)

  def force(len: Int): Column[A] =
    DenseColumn.force(f, len)

  def mask(mask: Mask): Column[A] = EvalColumn { row =>
    if (mask(row)) NA else f(row)
  }

  def setNA(naRow: Int): Column[A] = EvalColumn { row =>
    if (row == naRow) NA else f(row)
  }

  def memoize(optimistic: Boolean): Column[A] =
    if (optimistic) new OptimisticMemoizingColumn(f)
    else new PessimisticMemoizingColumn(f)
}

private[column] sealed trait MemoizingColumn[A] extends BoxedColumn[A] {
  private def eval: EvalColumn[A] = EvalColumn(apply _)
  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = eval.cellMap(f)
  def reindex(index: Array[Int]): Column[A] = eval.reindex(index)
  def force(len: Int): Column[A] = eval.force(len)
  def mask(mask: Mask): Column[A] = eval.mask(mask)
  def setNA(naRow: Int): Column[A] = eval.setNA(naRow)
  def memoize(optimistic: Boolean): Column[A] = this
}

private[column] class OptimisticMemoizingColumn[A](get: Int => Cell[A]) extends MemoizingColumn[A] {
  private val cached: ConcurrentHashMap[Int, Cell[A]] = new ConcurrentHashMap()

  def apply(row: Int): Cell[A] = {
    if (!cached.containsKey(row))
      cached.putIfAbsent(row, get(row))
    cached.get(row)
  }
}

private[column] class PessimisticMemoizingColumn[A](get: Int => Cell[A]) extends MemoizingColumn[A] {
  private val cached: ConcurrentHashMap[Int, Box] = new ConcurrentHashMap()

  def apply(row: Int): Cell[A] = {
    if (!cached.containsKey(row))
      cached.putIfAbsent(row, new Box(row))
    cached.get(row).cell
  }

  // A Box let's us do the double-checked locking per-value, rather than having
  // to lock the entire cache for the update.
  private class Box(row: Int) {
    @volatile var _cell: Cell[A] = null
    def cell: Cell[A] = {
      var result = _cell
      if (result == null) {
        synchronized {
          result = _cell
          if (result == null) {
            _cell = get(row); result = _cell
          }
        }
      }
      result
    }
  }
}

private[column] final class EmptyColumn[A] extends BoxedColumn[A] {
  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = EvalColumn(row => f(NA))
  def apply(row: Int): Cell[A] = NA
  def mask(mask: Mask): Column[A] = this
  def setNA(row: Int): Column[A] = this
  def reindex(index: Array[Int]): Column[A] = this
  def force(len: Int): Column[A] = this
  def memoize(optimistic: Boolean): Column[A] = this
}

private[column] sealed trait DenseColumn[@sp(Int,Long,Double) A] extends UnboxedColumn[A] {
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

  def setNA(row: Int): Column[A] =
    if ((row < 0 || row > values.length) && !nmValues(row)) this
    else Column.dense(values, naValues + row, nmValues - row).asInstanceOf[Column[A]]

  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = Column.eval(apply _).cellMap(f)

  def memoize(optimistic: Boolean): Column[A] = this

  override def toString: String = {
    val len = nmValues.max.map(_ + 1).getOrElse(values.length)
    (0 until len).map(apply(_).toString).mkString("Column(", ", ", ")")
  }
}

private[column] object DenseColumn extends DenseColumnFunctions

private[column] case class IntColumn(values: Array[Int], naValues: Mask, nmValues: Mask) extends DenseColumn[Int] {
  def valueAt(row: Int): Int = values(row)
  def map[@sp(Int,Long,Double) B](f: Int => B): Column[B] = DenseColumn.mapInt(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Int] = DenseColumn.reindexInt(index, values, naValues, nmValues)
  def force(len: Int): Column[Int] = IntColumn(
    java.util.Arrays.copyOf(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

private[column] case class LongColumn(values: Array[Long], naValues: Mask, nmValues: Mask) extends DenseColumn[Long] {
  def valueAt(row: Int): Long = values(row)
  def map[@sp(Int,Long,Double) B](f: Long => B): Column[B] = DenseColumn.mapLong(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Long] = DenseColumn.reindexLong(index, values, naValues, nmValues)
  def force(len: Int): Column[Long] = LongColumn(
    java.util.Arrays.copyOf(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

private[column] case class DoubleColumn(values: Array[Double], naValues: Mask, nmValues: Mask) extends DenseColumn[Double] {
  def valueAt(row: Int): Double = values(row)
  def map[@sp(Int,Long,Double) B](f: Double => B): Column[B] = DenseColumn.mapDouble(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Double] = DenseColumn.reindexDouble(index, values, naValues, nmValues)
  def force(len: Int): Column[Double] = DoubleColumn(
    java.util.Arrays.copyOf(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

private[column] case class AnyColumn[A](values: Array[Any], naValues: Mask, nmValues: Mask) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row).asInstanceOf[A]
  def map[@sp(Int,Long,Double) B](f: A => B): Column[B] = DenseColumn.mapAny(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[A] = DenseColumn.reindexAny(index, values, naValues, nmValues)
  def force(len: Int): Column[A] = AnyColumn(
    DenseColumn.copyArray(values, len),
    if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
    nmValues.filter(_ < len)
  )
}

private[column] case class GenericColumn[A](values: Array[A], naValues: Mask, nmValues: Mask) extends DenseColumn[A] {
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
