/*  _____                    _
 * |  ___| __ __ _ _ __ ___ (_) __ _ _ __
 * | |_ | '__/ _` | '_ ` _ \| |/ _` | '_ \
 * |  _|| | | (_| | | | | | | | (_| | | | |
 * |_|  |_|  \__,_|_| |_| |_|_|\__,_|_| |_|
 *
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package framian

import scala.collection.SortedMap
import scala.reflect.ClassTag

import spire.algebra._
import spire.implicits._
import spire.compat._
import shapeless.{Generic, HList, HNil, Typeable, Poly2, LUBConstraint, UnaryTCConstraint}
import shapeless.ops.hlist.{LeftFolder, Length}
import shapeless.syntax.typeable._
import shapeless.syntax._

import framian.reduce.Reducer
import framian.column._

import Index.GenericJoin.Skip

trait Frame[Row, Col] {
  /** The row keys' index. */
  def rowIndex: Index[Row]

  /** The column keys' index. */
  def colIndex: Index[Col]

  implicit def rowClassTag = rowIndex.classTag
  implicit def rowOrder = rowIndex.order
  implicit def colClassTag = colIndex.classTag
  implicit def colOrder = colIndex.order

  /** A column-oriented view of this frame. Largely used internally. */
  def columnsAsSeries: Series[Col, UntypedColumn]

  /** A row-oriented view of this frame. Largely used internally. */
  def rowsAsSeries: Series[Row, UntypedColumn]

  /**
   * Returns `true` if this frame can be treated as column oriented. This is
   * largely for optimization purposes.
   */
  def isColOriented: Boolean

  /**
   * Returns `true` if this frame can be treated as row oriented. This is
   * largely for optimization purposes.
   */
  def isRowOriented: Boolean

  /**
   * Transposes the rows and columns of this frame. All rows in this frame
   * becomes the columns of the new frame.
   */
  def transpose: Frame[Col, Row]

  /**
   * Returns the set of all unique column keys.
   */
  def colKeys: Set[Col] = colIndex.map(_._1)(collection.breakOut)

  /**
   * Returns the set of all unique row keys.
   */
  def rowKeys: Set[Row] = rowIndex.map(_._1)(collection.breakOut)

  /**
   * Returns the number of rows in this frame.
   */
  def rows: Int = rowIndex.size

  /**
   * Returns the number of cols in this frame.
   */
  def cols: Int = colIndex.size

  /**
   * Returns `true` if this frame is logically empty. A frame is logically
   * empty if none of its rows or columns contain a value, though they may
   * contain [[NA]]s or [[NM]]s.
   *
   * TODO: I think an NM should also count as a "value".
   */
  def isEmpty =
    columnsAsSeries.iterator.collectFirst {
      case (id, Value(column)) if Series(rowIndex, column.cast[Any]).hasValues => id
    }.isEmpty

  /** The following methods allow a user to apply reducers directly across a frame. In
    * particular, this API demands that we specify the type that the reducer accepts and
    * it will only apply it in the case that there exists a type conversion for a given
    * column.
    */
  def reduceFrame[V: ColumnTyper, R: ClassTag: ColumnTyper](reducer: Reducer[V, R]): Series[Col, R] =
    columnsAsSeries.flatMapCell { col =>
      Series(rowIndex, col.cast[V]).reduce(reducer)
    }

  def reduceFrameByKey[V: ColumnTyper, R: ClassTag: ColumnTyper](reducer: Reducer[V, R]): Frame[Row, Col] =
    columnsAsSeries.cellMap {
      case NA => Value(Series(rowIndex, Column.empty[V]()))
      case Value(col) => Value(Series(rowIndex, col.cast[V]))
      case NM => NM
    }.denseIterator.foldLeft(Frame.empty[Row, Col]) { case (acc, (key, series)) =>
      acc.join(key, series.reduceByKey(reducer))(Join.Outer)
    }

  def reduceFrameWithCol[A: ColumnTyper, B: ColumnTyper, C: ClassTag](col: Col)(reducer: Reducer[(A, B), C]): Series[Col, C] = {
    val fixed = column[A](col)
    Series.fromCells(columnsAsSeries.to[List].collect {
      case (k, Value(untyped)) if k != col =>
        val series = Series(rowIndex, untyped.cast[B])
        k -> (fixed zip series).reduce(reducer)
    }: _*)
  }

  def getColumnGroup(col: Col): Frame[Row, Col] =
    withColIndex(colIndex.getAll(col))

  def getRowGroup(row: Row): Frame[Row, Col] =
    withRowIndex(rowIndex.getAll(row))

  def mapRowGroups[R1: ClassTag: Order, C1: ClassTag: Order](f: (Row, Frame[Row, Col]) => Frame[R1, C1]): Frame[R1, C1] = {
    object grouper extends Index.Grouper[Row] {
      case class State(rows: Int, keys: Vector[Array[R1]], cols: Series[C1, UntypedColumn]) {
        def result(): Frame[R1, C1] = ColOrientedFrame(Index(Array.concat(keys: _*)), cols)
      }

      def init = State(0, Vector.empty, Series.empty)
      def group(state: State)(keys: Array[Row], indices: Array[Int], start: Int, end: Int): State = {
        val groupRowIndex = Index(keys.slice(start, end), indices.slice(start, end))
        val groupKey = keys(start)
        val group = ColOrientedFrame(groupRowIndex, columnsAsSeries)

        val State(offset, groupKeys, cols) = state
        val result = f(groupKey, group)
        val newCols = cols.merge(result.columnsAsSeries mapValues {
          _.reindex(result.rowIndex.indices).shift(offset)
        })
        State(offset + result.rowIndex.size, groupKeys :+ result.rowIndex.keys, newCols)
      }
    }

    Index.group(rowIndex)(grouper).result()
  }

  // Row/Column Index manipulation.

  /** Replaces the column index with `index`. */
  def withColIndex[C1](index: Index[C1]): Frame[Row, C1]

  /** Replaces the row index with `index`. */
  def withRowIndex[R1](index: Index[R1]): Frame[R1, Col]

  /**
   * Put the columns in sorted order. This affects only the traversal order of
   * the columns.
   */
  def sortColumns: Frame[Row, Col] = withColIndex(colIndex.sorted)

  /**
   * Put the rows in sorted order. This affects only the traversal order of the
   * rows.
   */
  def sortRows: Frame[Row, Col] = withRowIndex(rowIndex.sorted)

  /**
   * Reverse the traversal order of the columns in this frame.
   */
  def reverseColumns: Frame[Row, Col] =
    withColIndex(colIndex.reverse)

  /**
   * Reverse the traversal order of the rows in this frame.
   */
  def reverseRows: Frame[Row, Col] =
    withRowIndex(rowIndex.reverse)

  /**
   * Removes rows whose row key is true for the predicate `p`.
   */
  def filterRowKeys(p: Row => Boolean) =
    withRowIndex(rowIndex.filter { case (row, _) => p(row) })

  /**
   * Removes rows whose row key is true for the predicate `p`.
   */
  def filterColKeys(p: Col => Boolean) =
    withColIndex(colIndex.filter { case (col, _) => p(col) })

  /**
   * Map the row index using `f`. This retains the traversal order of the rows.
   */
  def mapRowKeys[R: Order: ClassTag](f: Row => R): Frame[R, Col] =
    this.withRowIndex(rowIndex.map { case (k, v) => (f(k), v) })

  /**
   * Map the column index using `f`. This retains the traversal order of the
   * columns.
   */
  def mapColKeys[C: Order: ClassTag](f: Col => C): Frame[Row, C] =
    this.withColIndex(colIndex.map { case (k, v) => (f(k), v) })

  /**
   * Retain only the cols in `cols`, dropping all others.
   */
  def retainColumns(cols: Col*): Frame[Row, Col] =
    filterColKeys(cols.toSet)

  /**
   * Retain only the rows in `rows`, dropping all others.
   */
  def retainRows(rows: Row*): Frame[Row, Col] =
    filterRowKeys(rows.toSet)

  /**
   * Drop the columns `cols` from the column index. This simply removes the
   * columns from the column index and does not modify the actual columns.
   */
  def dropColumns(cols: Col*): Frame[Row, Col] =
    filterColKeys(k => !cols.contains(k))

  /**
   * Drop the rows `rows` from the row index. This simply removes the rows
   * from the index and does not modify the actual columns.
   */
  def dropRows(rows: Row*): Frame[Row, Col] =
    filterRowKeys(k => !rows.contains(k))

  /**
   * Returns the value of the cell at row `rowKey` and column `colKey` as the
   * type `A`. If the value doesn't exist, then [[NA]] is returned. If the
   * value is not meaningful as an instance of `A`, then [[NM]] is returned.
   */
  def apply[A: ColumnTyper](rowKey: Row, colKey: Col): Cell[A] = for {
    col <- columnsAsSeries(colKey)
    row <- Cell.fromOption(rowIndex get rowKey)
    a <- col.cast[A].apply(row)
  } yield a

  /** Returns a single column from this `Frame` cast to type `T`. */
  def column[T: ColumnTyper](col: Col): Series[Row, T] = get(Cols(col).as[T])

  /** Returns a single row from this `Frame` cast to type `T`. */
  def row[T: ColumnTyper](row: Row): Series[Col, T] = get(Rows(row).as[T])

  def getRow(key: Row): Option[Rec[Col]] = rowIndex.get(key) map Rec.fromRow(this)

  def getCol(key: Col): Option[Rec[Row]] = colIndex.get(key) map Rec.fromCol(this)

  // Cols/Rows based ops.

  /**
   * Extract values from the columns of the series using `rows` and returns
   * them in a [[Series]].
   */
  def get[A](rows: Rows[Row, A]): Series[Col, A] =
    Frame.extract(colIndex, rowsAsSeries, rows)

  /**
   * Extract values from the rows of the series using `cols` and returns them
   * in a [[Series]].
   */
  def get[A](cols: Cols[Col, A]): Series[Row, A] =
    Frame.extract(rowIndex, columnsAsSeries, cols)

  /**
   * Re-indexes the frame using the extractor `cols` to define the new row
   * keys. This will not re-order the frame rows, just replace the keys. This
   * will drop any rows where `cols` extracts a [[NonValue]].
   *
   * To retain `NA`s or `NM`s in the index, you'll need to recover your
   * [[Cols]] with some other value. For example,
   *
   * {{{
   * frame.reindex(cols.map(Value(_)).recover { case nonValue => nonValue })
   * }}}
   */
  def reindex[A: Order: ClassTag](cols: Cols[Col, A]): Frame[A, Col] =
    withRowIndex(Frame.reindex(rowIndex, columnsAsSeries, cols))

  /**
   * Re-indexes the frame using the extractor `rows` to define the new row
   * keys. This will not re-order the frame cols, just replace the keys. This
   * will drop any columns where `rows` extracts a [[NonValue]].
   *
   * To retain `NA`s or `NM`s in the index, you'll need to recover your
   * [[Rows]] with some other value. For example,
   *
   * {{{
   * frame.reindex(rows.map(Value(_)).recover { case nonValue => nonValue })
   * }}}
   */
  def reindex[A: Order: ClassTag](rows: Rows[Row, A]): Frame[Row, A] =
    withColIndex(Frame.reindex(colIndex, rowsAsSeries, rows))

  /**
   * Maps each row to a value using `rows`, then maps the result with the
   * column key using `f` and stores it in the row `to`. If `to` doesn't exist
   * then a new row will be appended onto the frame, otherwise the row(s) with
   * key `to` will be removed first.
   */
  def mapWithIndex[A, B: ClassTag](rows: Rows[Row, A], to: Row)(f: (Col, A) => B): Frame[Row, Col] =
    transpose.mapWithIndex(rows.toCols, to)(f).transpose

  /**
   * Maps each column to a value using `cols`, then maps the result with the row
   * key using `f` and stores it in the column `to`. If `to` doesn't exist
   * then a new column will be appended onto the frame, otherwise the
   * columns(s) with key `to` will be removed first.
   */
  def mapWithIndex[A, B: ClassTag](cols: Cols[Col, A], to: Col)(f: (Row, A) => B): Frame[Row, Col] =
    dropColumns(to).merge(to, Frame.extract(rowIndex, columnsAsSeries, cols).mapValuesWithKeys(f))(Merge.Outer)

  /**
   * Extracts a row from this frame using [[Rows]], then merges it back into
   * this frame as the row `to`. If `to` doesn't exist then a new row will be
   * appended onto the frame, otherwise the row(s) with key `to` will be
   * removed first.
   */
  def map[A, B: ClassTag](rows: Rows[Row, A], to: Row)(f: A => B): Frame[Row, Col] =
    transpose.map(rows.toCols, to)(f).transpose

  /**
   * Extracts a column from this frame using [[Cols]], then merges it back into
   * this frame as the column `to`. If `to` doesn't exist then a new column
   * will be appended onto the frame, otherwise the columns(s) with key `to`
   * will be removed first.
   *
   * This is equivalent to, but may be more efficient than
   * `frame.merge(to, frame.get(cols))(Merge.Outer)`.
   */
  def map[A, B: ClassTag](cols: Cols[Col, A], to: Col)(f: A => B): Frame[Row, Col] =
    dropColumns(to).merge(to, Frame.extract(rowIndex, columnsAsSeries, cols.map(f)))(Merge.Outer)

  /**
   * Filter this frame using `cols` extract values and the predicate `p`. If,
   * for a given row, `p` is false, then that row will be removed from the
   * frame. Any [[NA]] and [[NM]] rows will also be removed.
   */
  def filter[A](cols: Cols[Col, A])(p: A => Boolean): Frame[Row, Col] = 
    withRowIndex(Frame.filter(rowIndex, columnsAsSeries, cols map p))

  /**
   * Filter this frame using `rows` extract values and the predicate `p`. If,
   * for a given column, `p` is false, then that column will be removed from the
   * frame. Any [[NA]] and [[NM]] columns will also be removed.
   */
  def filter[A](rows: Rows[Row, A])(p: A => Boolean): Frame[Row, Col] = 
    withColIndex(Frame.filter(colIndex, rowsAsSeries, rows map p))

  /**
   * Sorts the frame using the order for the [[Rows]] provided. This will only
   * ever permute the cols of the frame and will not remove/add anything.
   *
   * @param rows The column value extractor to get the sort key
   */
  def sortBy[A: Order](rows: Rows[Row, A]): Frame[Row, Col] =
    withColIndex(Frame.sortBy(colIndex, rowsAsSeries, rows))

  /**
   * Sorts the frame using the order for the [[Cols]] provided. This will only
   * ever permute the rows of the frame and will not remove/add anything.
   *
   * @param cols The row value extractor to get the sort key
   */
  def sortBy[A: Order](cols: Cols[Col, A]): Frame[Row, Col] =
    withRowIndex(Frame.sortBy(rowIndex, columnsAsSeries, cols))

  /**
   * This "groups" the frame rows using the [[Cols]] extractor to determine the
   * group for each row. Each row is then re-keyed using its group.
   *
   * This is equivalent to, but more efficient than, `frame.sortBy(cols).reindex(cols)`.
   */
  def group[A: Order: ClassTag](cols: Cols[Col, A]): Frame[A, Col] =
    withRowIndex(Frame.group(rowIndex, columnsAsSeries, cols))

  /**
   * This "groups" the frame cols using the [[Rows]] extractor to determine the
   * group for each column. Each column is then re-keyed using its group.
   *
   * This is equivalent to, but more efficient than, `frame.sortBy(rows).reindex(rows)`.
   */
  def group[A: Order: ClassTag](rows: Rows[Row, A]): Frame[Row, A] =
    withColIndex(Frame.group(colIndex, rowsAsSeries, rows))

  /**
   * Reduces this frame using `cols` and joins the result back into the frame.
   * This doesn't remove any rows from the frame; the reduced result is
   * duplicated for each row. This will replace the `to` column if it exists,
   * otherwise it creates a new column `to` at the end.
   *
   * {{{
   * scala&gt; val f = Frame.fromRows("a" :: 2 :: HNil, "b" :: 3 :: HNil)
   * scala&gt; f.reduce(Cols(1).as[Int], 1)(reduce.Sum)
   * res0: Frame[Int, Int] =
   *     0 . 1
   * 0 : a | 5
   * 1 : b | 5
   * }}}
   */
  def reduce[A, B: ClassTag](cols: Cols[Col, A], to: Col)(reducer: Reducer[A, B]): Frame[Row, Col] = {
    val cell = get(cols).reduce(reducer)
    val result = Series(rowIndex, Column.eval(_ => cell))
    dropColumns(to).merge(to, result)(Merge.Outer)
  }

  /**
   * Reduces this frame using `rows` and joins the result back into the frame.
   * This doesn't remove any columns from the frame; the reduced result is
   * duplicated for each column. This will replace the `to` row if it exists,
   * otherwise it creates a new row `to` at the end.
   *
   * {{{
   * scala&gt; val f = Frame.fromColumns("a" :: 2 :: HNil, "b" :: 3 :: HNil)
   * scala&gt; f.reduce(Cols(1).as[Int], 1)(reduce.Sum)
   * res0: framian.Frame[Int,Int] =
   *     0 . 1
   * 0 : a | b
   * 1 : 5 | 5
   * }}}
   */
  def reduce[A, B: ClassTag](rows: Rows[Row, A], to: Row)(reducer: Reducer[A, B]): Frame[Row, Col] =
    transpose.reduce(rows.toCols, to)(reducer).transpose

  /**
   * Reduces this frame, by row key groups, using `cols` and joins the result
   * back into the frame. Within each row key group, the reduced result is
   * duplicated for each row. If `to` exists it will be replaced, otherwise it
   * will be added to the end of the columns.
   */
  def reduceByKey[A, B: ClassTag](cols: Cols[Col, A], to: Col)(reducer: Reducer[A, B]): Frame[Row, Col] =
    dropColumns(to).join(to, get(cols).reduceByKey(reducer))(Join.Outer)

  /**
   * Reduces this frame, by column key groups, using `rows` and joins the
   * result back into the frame. Within each column key group, the reduced
   * result is duplicated for each column. If `to` exists it will be replaced,
   * otherwise it will be added to the end of the rows.
   */
  def reduceByKey[A, B: ClassTag](rows: Rows[Row, A], to: Row)(reducer: Reducer[A, B]): Frame[Row, Col] =
    transpose.reduceByKey(rows.toCols, to)(reducer).transpose

  /**
   * Appends the rows in `that` to the end of the rows in `this`. This will
   * force the columns into sorted order.
   */
  def appendRows(that: Frame[Row, Col]): Frame[Row, Col] =
    if (this.isColOriented) {
      val (keys0, indices0) = this.rowIndex.unzip
      val (keys1, indices1) = that.rowIndex.unzip
      val cols0 = this.columnsAsSeries.mapValues(_.reindex(indices0))
      val cols1 = that.columnsAsSeries.mapValues(_.reindex(indices1))
      val offset = keys0.size
      val cols = cols0.combine(cols1)(col => col, _.shift(offset), { (col0, col1) =>
        ConcatColumn(col0, col1, offset)
      })
      ColOrientedFrame(Index(keys0 ++ keys1), cols)
    } else {
      val merger = Merger[Col](Merge.Outer)
      val (keys, lIndices, rIndices) = Index.cogroup(this.colIndex, that.colIndex)(merger).result()
      val rows0 = this.rowsAsSeries.mapValues(_.reindex(lIndices))
      val rows1 = that.rowsAsSeries.mapValues(_.reindex(rIndices))
      RowOrientedFrame(Index(keys), rows0 ++ rows1)
    }

  /**
   * Appends the columns in `that` to the end of the columns in `this`. This
   * will force the rows into sorted order.
   */
  def appendCols(that: Frame[Row, Col]): Frame[Row, Col] =
    transpose.appendRows(that.transpose).transpose

  override def hashCode: Int = {
    val values = columnsAsSeries.iterator flatMap { case (colKey, cell) =>
      val col = cell.getOrElse(UntypedColumn.empty).cast[Any]
      Series(rowIndex, col).iterator map { case (rowKey, value) =>
        (rowKey, colKey, value)
      }
    }
    values.toList.hashCode
  }

  override def equals(that: Any) = that match {
    case (that: Frame[_, _]) =>
      val cols0 = this.columnsAsSeries
      val cols1 = that.columnsAsSeries
      val rowIndex0 = this.rowIndex
      val rowIndex1 = that.rowIndex
      val keys0 = rowIndex0 map (_._1)
      val keys1 = rowIndex1 map (_._1)
      (cols0.size == cols1.size) && (keys0 == keys1) && (cols0.iterator zip cols1.iterator).forall {
        case ((k0, v0), (k1, v1)) if k0 == k1 =>
          def col0 = v0.getOrElse(UntypedColumn.empty).cast[Any]
          def col1 = v1.getOrElse(UntypedColumn.empty).cast[Any]
          (v0 == v1) || (Series(rowIndex0, col0) == Series(rowIndex1, col1))

        case _ => false
      }

    case _ => false
  }

  override def toString: String = {
    def pad(repr: String, width: Int): String =
      repr + (" " * (width - repr.length))

    def justify(reprs: List[String]): List[String] = {
      val width = reprs.maxBy(_.length).length
      reprs map (pad(_, width))
    }

    def collapse(keys: List[String], cols: List[List[String]]): List[String] = {
      import scala.collection.immutable.`::`

      def loop(keys0: List[String], cols0: List[List[String]], lines: List[String]) :List[String] = {
        keys0 match {
          case key :: keys1 =>
            val row = cols0 map (_.head)
            val line = key + row.mkString(" : ", " | ", "")
            loop(keys1, cols0 map (_.tail), line :: lines)

          case Nil =>
            lines.reverse
        }
      }

      val header = keys.head + cols.map(_.head).mkString("   ", " . ", "")
      header :: loop(keys.tail, cols map (_.tail), Nil)
    }

    val keys = justify("" :: rowIndex.map(_._1.toString).toList)
    val cols = columnsAsSeries.iterator.map { case (key, cell) =>
      val header = key.toString
      val col = cell.getOrElse(UntypedColumn.empty).cast[Any]
      val values: List[String] = Series(rowIndex, col).iterator.map {
        case (_, Value(value)) => value.toString
        case (_, nonValue) => nonValue.toString
      }.toList
      justify(header :: values)
    }.toList

    collapse(keys, cols).mkString("\n")
  }

  private def genericJoin(that: Frame[Row, Col])(cogrouper: Index.GenericJoin[Row]): Frame[Row, Col] = {
    val state = Index.cogroup(this.rowIndex, that.rowIndex)(cogrouper)
    val (keys, lIndex, rIndex) = state.result()
    val lCols = this.columnsAsSeries.mapValues(_.setNA(Skip).reindex(lIndex))
    val rCols = that.columnsAsSeries.mapValues(_.setNA(Skip).reindex(rIndex))
    ColOrientedFrame(Index.ordered(keys), lCols ++ rCols)
  }

  def merge(that: Frame[Row, Col])(mergeStrategy: Merge): Frame[Row, Col] =
    genericJoin(that)(Merger(mergeStrategy))

  def merge[T: ClassTag](col: Col, that: Series[Row, T])(mergeStrategy: Merge): Frame[Row, Col] =
    merge(that.toFrame(col))(mergeStrategy)

  def merge[L <: HList](them: L)(merge: Merge)(implicit folder: Frame.SeriesMergeFolder[L, Row, Col]): Frame[Row, Col] =
    them.foldLeft(this)(Frame.mergeSeries)

  def join(that: Frame[Row, Col])(joinStrategy: Join): Frame[Row, Col] =
    genericJoin(that)(Joiner(joinStrategy))

  def join[T: ClassTag](col: Col, that: Series[Row, T])(joinStrategy: Join): Frame[Row, Col] =
    join(that.toFrame(col))(joinStrategy)

  def join[L <: HList](them: L)(join: Join)(implicit folder: Frame.SeriesJoinFolder[L, Row, Col]): Frame[Row, Col] =
    them.foldLeft(this)(Frame.joinSeries)

  def joinBy[A: Order: ClassTag](by: Cols[Col, A])(that: Frame[_, Col])(join: LeftBiasedJoin): Frame[Row, Col] = {
    val reIdx = reindex(by)
    val joined = reIdx.join(that.reindex(by))(join)
    joined.withRowIndex(
      joined.rowIndex.flatMap{ case (r, i) =>
        reIdx.rowIndex.getAll(r).indices.map(k => (rowIndex.keyAt(k), i))
      }.unzip match {
        case (k, v) => Index.ordered(k, v)
      }
    )
  }
}

object Frame {

  /**
   * Create an empty `Frame` with no values.
   */
  def empty[Row: ClassTag: Order, Col: ClassTag: Order]: Frame[Row, Col] =
    ColOrientedFrame[Row, Col](Index.empty[Row], Index.empty[Col], Column.empty())

  /**
   * Populates a homogeneous `Frame` given the rows/columns of the table. The
   * value of each cell is calculated using `f`, applied to its row and column
   * index.
   *
   * For instance, we can make a multiplication table,
   *
   * {{{
   * Frame.fill(1 to 9, 1 to 9) { (row, col) => Value(row * col) }
   * }}}
   */
  def fill[A: Order: ClassTag, B: Order: ClassTag, C: ClassTag](rows: Iterable[A], cols: Iterable[B])(f: (A, B) => Cell[C]): Frame[A, B] = {
    val rows0 = rows.toVector
    val cols0 = cols.toVector
    val columns = Column.dense(cols0.map { b =>
      TypedColumn(Column(rows0.map { a => f(a, b) }: _*)): UntypedColumn
    }.toArray)
    ColOrientedFrame(Index.fromKeys(rows0: _*), Index.fromKeys(cols0: _*), columns)
  }

  /**
   * Construct a Frame whose rows are populated from some type `A`. Row
   * populators may exist for things like JSON objects or Shapeless Generic
   * types. For example,
   *
   * {{{
   * scala&gt; case class Person(name: String, age: Int)
   * scala&gt; val Alice = Person("Alice", 42)
   * scala&gt; val Bob = Person("Alice", 23)
   * scala&gt; Frame.fromRows(Alice, Bob)
   * res0: Frame[Int, Int] =
   *     0     . 1
   * 0 : Alice | 42
   * 1 : Bob   | 23
   * }}}
   *
   * TODO: This should really take the row too (eg. `rows: (Row, A)*`).
   */
  def fromRows[A, Col: ClassTag](rows: A*)(implicit pop: RowPopulator[A, Int, Col]): Frame[Int, Col] =
    pop.frame(rows.zipWithIndex.foldLeft(pop.init) { case (state, (data, row)) =>
      pop.populate(state, row, data)
    })

  /**
   * Construct a Frame whose columns are populated from some type `A`. Column
   * populators may exist for things like JSON objects or Shapeless Generic
   * types. For example,
   *
   * {{{
   * scala&gt; case class Person(name: String, age: Int)
   * scala&gt; val Alice = Person("Alice", 42)
   * scala&gt; val Bob = Person("Alice", 23)
   * scala&gt; Frame.fromRows(Alice, Bob)
   * res0: Frame[Int, Int] =
   *     0     . 1
   * 0 : Alice | Bob
   * 1 : 42    | 23
   * }}}
   */
  def fromColumns[A, Row: ClassTag](cols: A*)(implicit pop: RowPopulator[A, Int, Row]): Frame[Row, Int] =
    fromRows[A, Row](cols: _*).transpose

  // Here by dragons, devoid of form...

  /** A polymorphic function for joining many [[Series]] into a `Frame`. */
  object joinSeries extends Poly2 {
    implicit def colSeriesPair[A: ClassTag: ColumnTyper, Row, Col] =
      at[Frame[Row, Col], (Col, Series[Row, A])] { case (frame, (col, series)) =>
        frame.join(col, series)(Join.Outer)
      }
  }

  /** A polymorphic function for merging many [[Series]] into a `Frame`. */
  object mergeSeries extends Poly2 {
    implicit def colSeriesPair[A: ClassTag: ColumnTyper, Row, Col] =
      at[Frame[Row, Col], (Col, Series[Row, A])] { case (frame, (col, series)) =>
        frame.merge(col, series)(Merge.Outer)
      }
  }

  /** A left fold on an HList that creates a Frame from a set of [[Series]]. */
  type SeriesJoinFolder[L <: HList, Row, Col] = LeftFolder.Aux[L, Frame[Row, Col], joinSeries.type, Frame[Row, Col]]

  /** A left fold on an HList that creates a Frame from a set of [[Series]]. */
  type SeriesMergeFolder[L <: HList, Row, Col] = LeftFolder.Aux[L, Frame[Row, Col], mergeSeries.type, Frame[Row, Col]]

  /** Implicit to help with inference of Row/Col in mergeColumn/mergeRows. Please ignore... */
  trait KeySeriesPair[L <: HList, Row, Col]
  object KeySeriesPair {
    import shapeless.::
    implicit def hnilKeySeriesPair[Row, Col] = new KeySeriesPair[HNil, Row, Col] {}
    implicit def hconsKeySeriesPair[A, T <: HList, Row, Col](implicit
        t: KeySeriesPair[T, Row, Col]) = new KeySeriesPair[(Col, Series[Row, A]) :: T, Row, Col] {}
  }

  /**
   * Given an HList of `(Col, Series[Row, V])` pairs, this will build a
   * `Frame[Row, Col]`, outer-merging all of the series together as the columns
   * of the frame.
   *
   * The use of `Generic.Aux` allows us to use auto-tupling (urgh) to allow
   * things like `Frame.mergeColumns("a" -> seriesA, "b" -> seriesB)`, rather
   * than having to use explicit `HList`s.
   *
   * @usecase def mergeColumn[Row, Col](cols: (Col, Series[Row, _])*): Frame[Row, Col]
   */
  def mergeColumns[S, L <: HList, Col, Row](cols: S)(implicit
      gen: Generic.Aux[S, L],
      ev: KeySeriesPair[L, Row, Col],
      folder: SeriesMergeFolder[L, Row, Col],
      ctCol: ClassTag[Col], orderCol: Order[Col],
      ctRow: ClassTag[Row], orderRow: Order[Row]): Frame[Row, Col] =
    gen.to(cols).foldLeft(Frame.empty[Row, Col])(mergeSeries)

  /**
   * Given an HList of `(Row, Series[Col, V])` pairs, this will build a
   * `Frame[Row, Col]`, outer-merging all of the series together as the rows
   * of the frame.
   *
   * The use of `Generic.Aux` allows us to use auto-tupling (urgh) to allow
   * things like `Frame.mergeRows("a" -> seriesA, "b" -> seriesB)`, rather
   * than having to use explicit `HList`s.
   *
   * @usecase def mergeRows[Row, Col](rows: (Row, Series[Col, _])*): Frame[Row, Col]
   */
  def mergeRows[S, L <: HList, Col, Row](rows: S)(implicit
      gen: Generic.Aux[S, L],
      ev: KeySeriesPair[L, Col, Row],
      folder: SeriesMergeFolder[L, Col, Row],
      ctCol: ClassTag[Col], orderCol: Order[Col],
      ctRow: ClassTag[Row], orderRow: Order[Row]): Frame[Row, Col] =
    gen.to(rows).foldLeft(Frame.empty[Col, Row])(mergeSeries).transpose

  // Axis-agnostic frame operations.

  private def reindex[I, K, A: Order: ClassTag](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, A]): Index[A] = {
    import scala.collection.mutable.ArrayBuffer

    val bldr = Index.newBuilder[A]
    sel.foreach(index, cols) { (_, row, cell) =>
      cell.foreach(bldr.add(_, row))
    }
    bldr.result()
  }

  private def sortBy[I: ClassTag: Order, K, A: Order](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, A]): Index[I] = {
    import spire.compat._
    import scala.collection.mutable.ArrayBuffer

    var buffer: ArrayBuffer[(Cell[A], I, Int)] = new ArrayBuffer
    sel.foreach(index, cols) { (key, row, sortKey) =>
      buffer += ((sortKey, key, row))
    }
    val bldr = Index.newBuilder[I]
    val pairs = buffer.toArray; pairs.qsortBy(_._1)
    cfor(0)(_ < pairs.length, _ + 1) { i =>
      val (_, key, idx) = pairs(i)
      bldr.add(key, idx)
    }
    bldr.result()
  }

  private def group[I, K, A: Order: ClassTag](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, A]): Index[A] = {
    val bldr = Index.newBuilder[A]
    sel.foreach(index, cols) { (_, row, cell) =>
      cell.foreach(bldr.add(_, row))
    }
    bldr.result().sorted
  }

  private def extract[I, K, A](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, A]): Series[I, A] = {
    val bldr = Column.newBuilder[A]()
    sel.foreach(index, cols) { (_, _, cell) =>
      bldr += cell
    }
    Series(index.resetIndices, bldr.result())
  }

  private def filter[I: Order: ClassTag, K](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, Boolean]): Index[I] = {
    val bldr = Index.newBuilder[I]
    sel.foreach(index, cols) {
      case (key, row, Value(true)) => bldr.add(key, row)
      case _ =>
    }
    bldr.result()
  }
}

case class ColOrientedFrame[Row, Col](
      rowIndex: Index[Row],
      colIndex: Index[Col],
      valueCols: Column[UntypedColumn])
    extends Frame[Row, Col] {

  def columnsAsSeries: Series[Col, UntypedColumn] = Series(colIndex, valueCols)
  def rowsAsSeries: Series[Row, UntypedColumn] = Series(rowIndex, Column.eval { row =>
    Value(RowView(colIndex, valueCols, row))
  }.memoize())

  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1] =
    ColOrientedFrame(rowIndex, ci, valueCols)

  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col] =
    ColOrientedFrame(ri, colIndex, valueCols)

  def transpose: Frame[Col, Row] = RowOrientedFrame(colIndex, rowIndex, valueCols)

  def isColOriented: Boolean = true
  def isRowOriented: Boolean = false
}

object ColOrientedFrame {
  def apply[Row, Col](rowIdx: Index[Row], cols: Series[Col, UntypedColumn]): Frame[Row, Col] =
    ColOrientedFrame(rowIdx, cols.index, cols.column)
}

case class RowOrientedFrame[Row, Col](
      rowIndex: Index[Row],
      colIndex: Index[Col],
      valueRows: Column[UntypedColumn])
    extends Frame[Row, Col] {

  def rowsAsSeries: Series[Row, UntypedColumn] = Series(rowIndex, valueRows)
  def columnsAsSeries: Series[Col, UntypedColumn] = Series(colIndex, Column.eval { row =>
    Value(RowView(colIndex, valueRows, row))
  }.memoize())
  

  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1] =
    RowOrientedFrame(rowIndex, ci, valueRows)

  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col] =
    RowOrientedFrame(ri, colIndex, valueRows)

  def transpose: Frame[Col, Row] = ColOrientedFrame(colIndex, rowIndex, valueRows)

  def isColOriented: Boolean = false
  def isRowOriented: Boolean = true
}

object RowOrientedFrame {
  def apply[Row, Col](colIdx: Index[Col], rows: Series[Row, UntypedColumn]): Frame[Row, Col] =
    RowOrientedFrame(rows.index, colIdx, rows.column)
}

private final case class RowView[K](
      index: Index[K],
      cols: Column[UntypedColumn],
      row: Int,
      trans: UntypedColumn => UntypedColumn = RowView.DefaultTransform
    ) extends UntypedColumn {

  def cast[B: ColumnTyper]: Column[B] = Column.eval[B] { colIdx =>
    for {
      col <- if (colIdx >= 0 && colIdx < index.size) cols(index.indexAt(colIdx))
             else NA
      value <- trans(col).cast[B].apply(row)
    } yield value
  }
  private def transform(f: UntypedColumn => UntypedColumn) = new RowView(index, cols, row, trans andThen f)
  def mask(na: Mask): UntypedColumn = transform(_.mask(na))
  def shift(rows: Int): UntypedColumn = transform(_.shift(rows))
  def reindex(index: Array[Int]): UntypedColumn = transform(_.reindex(index))
  def setNA(na: Int): UntypedColumn = transform(_.setNA(na))
}

private object RowView {
  val DefaultTransform: UntypedColumn => UntypedColumn = a => a
}
