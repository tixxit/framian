package pellucid
package pframe

import spire.algebra._
import spire.syntax.additiveMonoid._

import shapeless._
import shapeless.ops.function._

/**
 * Wraps a collection of columns and a source [[Frame]]. This let's us perform
 * operations on the frame in a column oriented manner.
 *
 * `ColumnSelector`s have an additional type parameter, aside from [[Frame]]'s
 * normal `Row` and `Col`; they have a `Sz &lt;: Size` parameter. This is used to
 * keep track of sizing information for `cols`. This is used to help select the
 * [[RowExtractor]] that is used for many operations. Most importantly, if we
 * lose sizing information (and so `Sz =:= Variable`), then this severely
 * limits the operations we can perform. Anything requiring `HList`s is out,
 * which includes `map` and `filter` for instance. However, we can still cast
 * the selector (via `as`) to types that don't require a fixed size, such as
 * lists of strings or JSON objects.
 */
final case class ColumnSelector[Row, Col, Sz <: Size](frame: Frame[Row, Col], cols: List[Col]) {
  import Nat._

  type RowExtractorAux[A] = RowExtractor[A, Col, Sz]

  /**
   * Constructs a `ColumnSelector` from an unsized collection of columns. This
   * means the selector will have only a [[Variable]] size, which limits the
   * operations that can be performed on it. If possible, one of the sized
   * variants should be used instead.
   */
  def apply(cols: Seq[Col]): ColumnSelector[Row, Col, Variable] =
    new ColumnSelector[Row, Col, Variable](frame, cols.toList)

  def apply(col: Col): ColumnSelector[Row, Col, Fixed[_1]] =
    new ColumnSelector[Row, Col, Fixed[_1]](frame, col :: Nil)

  def apply(col0: Col, col1: Col): ColumnSelector[Row, Col, Fixed[_2]] =
    new ColumnSelector[Row, Col, Fixed[_2]](frame, col0 :: col1 :: Nil)

  def apply(col0: Col, col1: Col, col2: Col): ColumnSelector[Row, Col, Fixed[_3]] =
    new ColumnSelector[Row, Col, Fixed[_3]](frame, col0 :: col1 :: col2 :: Nil)

  def apply[N <: Nat](sized: Sized[Iterable[Col], N]): ColumnSelector[Row, Col, Fixed[N]] =
    new ColumnSelector[Row, Col, Fixed[N]](frame, sized.unsized.toList)

  def get[A: RowExtractorAux](key: Row): Cell[A] = for {
    i <- Cell.fromOption(frame.rowIndex.get(key))
    extractor = RowExtractor[A, Col, Sz]
    p <- Cell.fromOption(extractor.prepare(frame, cols))
    x <- extractor.extract(frame, key, i, p)
  } yield x

  /**
   * Casts each row to type `A` and returns the result as a [[Series]].
   */
  def as[A: RowExtractorAux]: Series[Row, A] = {
    val extractor = RowExtractor[A, Col, Sz]
    val column = extractor.prepare(frame, cols).fold(Column.empty[A]) { p =>
      val cells = frame.rowIndex map { case (key, row) =>
        extractor.extract(frame, key, row, p)
      }
      Column.fromCells(cells.toVector)
    }
    Series(frame.rowIndex, column)
  }

  /**
   * Map each row using a function whose arity matches the size. This returns
   * the results in a [[Series]] that shares its row [[Index]] with the frame
   * and so can be efficiently joined back to the source [[Frame]].
   *
   * An example of using `map` given a [[Frame]]:
   * {{{
   * val frame: Frame[Int, String] = ...
   * val series = frame.columns("x", "y") map { (x: Double, y: Double) => x + y }
   * val newFrame = frame.join("z", series)
   * }}}
   */
  def map[F, L <: HList, A](f: F)(implicit fntop: FnToProduct.Aux[F, L => A],
      e: RowExtractor[L, Col, Sz]): Series[Row, A] = {
    val extractor = RowExtractor[L, Col, Sz]
    val column = extractor.prepare(frame, cols).fold(Column.empty[A]) { p =>
      val fn = fntop(f)
      val cells = frame.rowIndex map { case (key, row) =>
        extractor.extract(frame, key, row, p) map fn
      }
      Column.fromCells(cells.toVector)
    }
    Series(frame.rowIndex, column)
  }

  /**
   * Map each row using a function whose arity is 1 greater than the size. The
   * first argument to the function will be the row index, so it's type must
   * be `Row`. The results are returned as a [[Series]] whose [[Index]] is the
   * same as the source [[Frame]]'s row index.
   *
   * An example of using `map` given a [[Frame]]:
   * {{{
   * val frame: Frame[Int, String] = ...
   * val series = frame.columns("x") mapWithIndex { (i: Int, x: Double) => i * x }
   * val newFrame = frame.join("z", series)
   * }}}
   */
  def mapWithIndex[F, L <: HList, A](f: F)(implicit fntop: FnToProduct.Aux[F, (Row :: L) => A],
      e: RowExtractorAux[L]): Series[Row, A] = {
    val extractor = RowExtractor[L, Col, Sz]
    val column = extractor.prepare(frame, cols).fold(Column.empty[A]) { p =>
      val fn = fntop(f)
      val cells = frame.rowIndex map { case (key, row) =>
        extractor.extract(frame, key, row, p) map { tail => fn(key :: tail) }
      }
      Column.fromCells(cells.toVector)
    }
    Series(frame.rowIndex, column)
  }

  def filter[F, L <: HList](f: F)(implicit fntop: FnToProduct.Aux[F, L => Boolean],
      e: RowExtractorAux[L]): Frame[Row,Col] = {
    val extractor = RowExtractor[L, Col, Sz]
    val filteredIndex = extractor.prepare(frame, cols).fold(frame.rowIndex.empty) { p =>
      frame.rowIndex filter { case (key, row) =>
        val fn = fntop(f)
        extractor.extract(frame, key, row, p) map fn getOrElse false
      }
    }
    frame.withRowIndex(filteredIndex)
  }
}
