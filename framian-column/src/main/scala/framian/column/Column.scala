package framian
package column

import java.util.concurrent.ConcurrentHashMap

import scala.language.experimental.macros

import scala.{specialized => sp }
import scala.annotation.unspecialized
import scala.reflect.macros.Context

sealed trait Column[+A] { // TODO: Can't specialize in 2.10, but can in 2.11.

  // @unspecialized -- See TODO above.
  def foldRow[B](row: Int)(na: B, nm: B, f: A => B): B = macro Column.foldRowImpl[A, B]

  def apply(row: Int): Cell[A]

  def cellMap[B](f: Cell[A] => Cell[B]): Column[B]

  def map[@sp(Int,Long,Double) B](f: A => B): Column[B]

  def filter(p: A => Boolean): Column[A]

  /**
   * Returns a column that will fallback to `that` for any row that is [[NA]].
   * That is, row `i` is defined as `this(i) orElse that(i)`, though may be
   * more efficient.
   *
   * @param that the column to fallback on for NA values
   */
  def orElse[A0 >: A](that: Column[A0]): Column[A0]

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
    (0 to 5).map(apply(_).toString).mkString("Column(", ", ", ", ...)")
}

trait BoxedColumn[A] extends Column[A] {
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

trait UnboxedColumn[@sp(Int,Long,Double) A] extends Column[A] {
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
   * Construct a column whose `i`-th row is the `i`-th element in `cells`. All
   * other rows are [[NA]].
   */
  def apply[A: GenColumnBuilder](cells: Cell[A]*): Column[A] = {
    val bldr = newBuilder[A]()
    cells.foreach(bldr += _)
    bldr.result()
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

  /**
   * Returns a column that returns [[NM]] for any row in `nmValues` and [[NA]]
   * for all others.
   */
  def empty[A](nmValues: Mask): Column[A] =
    AnyColumn[A](new Array[Any](0), Mask.empty, nmValues)

  /**
   * A column that returns [[NA]] for all rows.
   */
  final object Empty extends BoxedColumn[Nothing] {
    def cellMap[B](f: Cell[Nothing] => Cell[B]): Column[B] = EvalColumn(row => f(NA))
    def apply(row: Int): Cell[Nothing] = NA
    def mask(mask: Mask): Column[Nothing] = this
    def setNA(row: Int): Column[Nothing] = this
    def reindex(index: Array[Int]): Column[Nothing] = this
    def force(len: Int): Column[Nothing] = this
    def memoize(optimistic: Boolean): Column[Nothing] = this
    def orElse[A0 >: Nothing](that: Column[A0]): Column[A0] = that
  }

  def foldRowImpl[A, B](c: Context)(row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Expr[B] =
    c.Expr(new ColumnMacros[c.type](c).foldRow(row)(na, nm, f))
}

class ColumnMacros[C <: /*blackbox.*/Context](val c: C) {
  import c.universe._

  def foldRow[A, B](row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Tree = {
    val cell = newTermName(c.fresh("foldRow$cell$"))
    val col = newTermName(c.fresh("foldRow$col$"))
    val value = newTermName(c.fresh("foldRow$value$"))
    val r = newTermName(c.fresh("foldRow$row$"))

    val tree = q"""
    ${c.prefix} match {
      case ($col: _root_.framian.column.UnboxedColumn[_]) =>
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
