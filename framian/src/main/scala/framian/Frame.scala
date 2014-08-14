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

import Index.GenericJoin.Skip

trait Frame[Row, Col] {
  def rowIndex: Index[Row]
  def colIndex: Index[Col]

  implicit def rowClassTag = rowIndex.classTag
  implicit def rowOrder = rowIndex.order
  implicit def colClassTag = colIndex.classTag
  implicit def colOrder = colIndex.order

  def columnsAsSeries: Series[Col, UntypedColumn]
  def rowsAsSeries: Series[Row, UntypedColumn]

  /**
   * Transposes the rows and columns of this frame. All rows in this frame
   * becomes the columns of the new frame.
   */
  def transpose: Frame[Col, Row] = TransposedFrame(this)

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

  def reduce[A, B: ClassTag](cols: Cols[Col, A], to: Col)(reducer: Reducer[A, B]): Frame[Row, Col] = {
    val series = get(cols)
    val cell = series.reduce(reducer)
    val result = Series(rowIndex, Column.wrap(_ => cell))
    this.merge(to, result)(Merge.Outer)
  }

  private def columnsAs[V: ColumnTyper]: Vector[(Col, Series[Row, V])] =
    columnsAsSeries.denseIterator.map { case (key, col) =>
      key -> Series(rowIndex, col.cast[V])
    }.toVector

  /** The following methods allow a user to apply reducers directly across a frame. In
    * particular, this API demands that we specify the type that the reducer accepts and
    * it will only apply it in the case that there exists a type conversion for a given
    * column.
    */
  def reduceFrame[V: ColumnTyper, R: ClassTag: ColumnTyper](reducer: Reducer[V, R]): Series[Col, R] =
    Series.fromCells(columnsAs[V].map { case (col, series) => col -> series.reduce(reducer) })

  def reduceFrameByKey[V: ColumnTyper, R: ClassTag: ColumnTyper](reducer: Reducer[V, R]): Frame[Row, Col] =
    columnsAs[V].foldLeft(Frame.empty[Row, Col]) { case (acc, (key, col)) =>
      acc.join(key, col.reduceByKey(reducer))(Join.Outer)
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

  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1]
  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col]

  def filter(f: Row => Boolean) = filterRowIndex(f)
  def filterRowIndex(f: Row => Boolean) = {
    val filteredRowIndex = rowIndex.filter { case (row, _) => f(row) }
    withRowIndex(filteredRowIndex)
  }

  def orderColumns: Frame[Row, Col] = withColIndex(colIndex.sorted)
  def orderRows: Frame[Row, Col] = withRowIndex(rowIndex.sorted)
  def orderRowsBy[O](rowOrder: Seq[O])(f: Row => O)(implicit order: Order[Row]): Frame[Row, Col] = {
    val orderLookup = rowOrder.zipWithIndex.toMap
    mapRowIndex { row => (orderLookup(f(row)), row) }.orderRows.mapRowIndex(_._2)
  }

  def reverseColumns: Frame[Row, Col] =
    withColIndex(colIndex.reverse)

  def reverseRows: Frame[Row, Col] =
    withRowIndex(rowIndex.reverse)

  /**
   * Map the row index using `f`. This retains the traversal order of the rows.
   */
  def mapRowIndex[R: Order: ClassTag](f: Row => R): Frame[R, Col] =
    this.withRowIndex(rowIndex.map { case (k, v) => (f(k), v) })

  /**
   * Map the column index using `f`. This retains the traversal order of the
   * columns.
   */
  def mapColIndex[C: Order: ClassTag](f: Col => C): Frame[Row, C] =
    this.withColIndex(colIndex.map { case (k, v) => (f(k), v) })

  /**
   * Retain only the rows in `rows`, dropping all others.
   */
  def retainRows(rows: Row*): Frame[Row, Col] = {
    val keep = rows.toSet
    withRowIndex(rowIndex filter { case (key, _) => keep(key) })
  }

  /**
   * Retain only the cols in `cols`, dropping all others.
   */
  def retainColumns(cols: Col*): Frame[Row, Col] = {
    val keep = cols.toSet
    withColIndex(colIndex filter { case (key, _) => keep(key) })
  }

  /**
    * Add an arbitrary sequence of values to your frame as a column.
    */
  def addColumn[T: ClassTag](col: Col, column: Seq[T]): Frame[Row, Col] =
    this.merge(col, Series(rowIndex, Column.fromArray(column.toArray)))(Merge.Outer)

  /**
   * Drop the columns `cols` from the column index. This simply removes the
   * columns from the column index and does not modify the actual columns.
   */
  def dropColumns(cols: Col*): Frame[Row, Col] =
    withColIndex(Index(colIndex.filter { case (col, _) => !cols.contains(col) } .toSeq: _*))

  /**
   * Drop the rows `rows` from the row index. This simply removes the rows
   * from the index and does not modify the actual columns.
   */
  def dropRows(rows: Row*): Frame[Row, Col] =
    withRowIndex(Index(rowIndex.filter { case (row, _) => !rows.contains(row) } .toSeq: _*))

  def apply[A: ColumnTyper](rowKey: Row, colKey: Col): Cell[A] = for {
    col <- columnsAsSeries(colKey)
    row <- Cell.fromOption(rowIndex get rowKey)
    a <- col.cast[A].apply(row)
  } yield a

  def column[T: ColumnTyper](c: Col): Series[Row, T] = get(Cols(c).as[T])

  def get[A](rows: Rows[Row, A]): Series[Col, A] =
    transpose.get(rows.toCols)

  def get[A](cols: Cols[Col, A]): Series[Row, A] = {
    val keys = cols getOrElse columnsAsSeries.index.keys.toList
    val column = cols.extractor.prepare(columnsAsSeries, keys).fold(Column.empty[A]) { p =>
      val cells = rowIndex map { case (key, row) =>
        cols.extractor.extract(row, p)
      }
      Column.fromCells(cells.toVector)
    }
    Series(rowIndex, column)
  }

  def getRow(key: Row): Option[Rec[Col]] = rowIndex.get(key) map Rec.fromRow(this)

  def getCol(key: Col): Option[Rec[Row]] = colIndex.get(key) map Rec.fromCol(this)

  /**
   * Re-indexes the frame using the extractor `cols` to define the new row
   * keys. This will not re-order the frame rows, just replace the keys. This
   * will drop any rows where `cols` extracts a [[NonValue]].
   *
   * To retain `NA`s or `NM`s in the index, you'll need to recover your
   * [[Cols]] with some other value. For example,
   *
   * {{{
   * frame.reindex(cols.map(Value(_)).recoverWith(nonValue => nonValue))
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
   * frame.reindex(rows.map(Value(_)).recoverWith(nonValue => nonValue))
   * }}}
   */
  def reindex[A: Order: ClassTag](rows: Rows[Row, A]): Frame[Row, A] =
    withColIndex(Frame.reindex(colIndex, rowsAsSeries, rows))

  /**
   * Maps each row to a value using `rows`, then maps the result with the
   * column key using `f` and stores it in a new row `to`.
   */
  def mapWithIndex[A, B: ClassTag](rows: Rows[Row, A], to: Row)(f: (Col, A) => B): Frame[Row, Col] =
    transpose.mapWithIndex(rows.toCols, to)(f).transpose

  /**
   * Maps each column to a value using `cols`, then maps the result with the row
   * key using `f` and stores it in a new column `to`.
   */
  def mapWithIndex[A, B: ClassTag](cols: Cols[Col, A], to: Col)(f: (Row, A) => B): Frame[Row, Col] =
    merge(to, Frame.extract(rowIndex, columnsAsSeries, cols).mapValuesWithKeys(f))(Merge.Outer)

  /**
   * Extracts a row from this frame using [[Rows]], then merges it back into
   * this frame as the row `to`.
   */
  def map[A, B: ClassTag](rows: Rows[Row, A], to: Row)(f: A => B): Frame[Row, Col] =
    transpose.map(rows.toCols, to)(f).transpose

  /**
   * Extracts a column from this frame using [[Cols]], then merges it back into
   * this frame as the column `to`.
   *
   * This is equivalent to, but may be more efficient than
   * `frame.merge(to, frame.get(cols))(Merge.Outer)`.
   */
  def map[A, B: ClassTag](cols: Cols[Col, A], to: Col)(f: A => B): Frame[Row, Col] =
    merge(to, Frame.extract(rowIndex, columnsAsSeries, cols.map(f)))(Merge.Outer)

  def filter[A](cols: Cols[Col, A])(f: A => Boolean): Frame[Row, Col] = {
    val extractor = cols.extractor
    val keys = cols getOrElse columnsAsSeries.index.keys.toList
    withRowIndex(extractor.prepare(columnsAsSeries, keys).fold(rowIndex.empty) { p =>
      rowIndex filter { case (key, row) =>
        extractor.extract(row, p) map f getOrElse false
      }
    })
  }

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

  private def genericJoin[T](
    getIndex: T => Index[Row],
    reindexColumns: (Array[Row], Array[Int], Array[Int]) => T => Seq[(Col, UntypedColumn)]
  )(that: T)(genericJoiner: Index.GenericJoin[Row]): Frame[Row, Col] = {
    // TODO: This should use simpler things, like:
    //   this.reindex(lIndex).withRowIndex(newRowIndex) ++
    //   that.reindex(rIndex).withRowIndex(newRowIndex)
    val res: genericJoiner.State = Index.cogroup(this.rowIndex, getIndex(that))(genericJoiner)
    val (keys, lIndex, rIndex) = res.result()
    val newRowIndex = Index.ordered(keys)
    val cols0 = this.columnsAsSeries.denseIterator.map { case (key, col) =>
      (key, col.setNA(Skip).reindex(lIndex))
    } .toSeq
    val cols1 = reindexColumns(keys, lIndex, rIndex)(that)
    val (newColIndex, cols) = (cols0 ++ cols1).unzip

    ColOrientedFrame(newRowIndex, Index(newColIndex.toArray), Column.fromArray(cols.toArray))
  }

  def merge(that: Frame[Row, Col])(mergeStrategy: Merge): Frame[Row, Col] =
    genericJoin[Frame[Row, Col]](
      { frame: Frame[Row, Col] => frame.rowIndex },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => frame: Frame[Row, Col] =>
        frame.columnsAsSeries.denseIterator.map { case (key, col) =>
          (key, col.setNA(Skip).reindex(rIndex))
        } .toSeq }
    )(that)(Merger[Row](mergeStrategy)(rowIndex.classTag))

  def merge[T: ClassTag: ColumnTyper](col: Col, that: Series[Row, T])(mergeStrategy: Merge): Frame[Row, Col] =
    genericJoin[Series[Row, T]](
      { series: Series[Row, T] => series.index },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => series: Series[Row, T] =>
        Seq((col, TypedColumn(series.column.setNA(Skip).reindex(rIndex)))) }
    )(that)(Merger[Row](mergeStrategy)(rowIndex.classTag))

  def merge[L <: HList](them: L)(merge: Merge)(implicit folder: Frame.SeriesMergeFolder[L, Row, Col]): Frame[Row, Col] =
    them.foldLeft(this)(Frame.mergeSeries)

  def join(that: Frame[Row, Col])(joinStrategy: Join): Frame[Row, Col] =
    genericJoin[Frame[Row, Col]](
      { frame: Frame[Row, Col] => frame.rowIndex },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => frame: Frame[Row, Col] =>
        frame.columnsAsSeries.denseIterator.map { case (key, col) =>
          (key, col.setNA(Skip).reindex(rIndex))
        } .toSeq }
    )(that)(Joiner[Row](joinStrategy)(rowIndex.classTag))

  def join[T: ClassTag: ColumnTyper](col: Col, that: Series[Row, T])(joinStrategy: Join): Frame[Row, Col] =
    genericJoin[Series[Row, T]](
      { series: Series[Row, T] => series.index },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => series: Series[Row, T] =>
        Seq((col, TypedColumn(series.column.setNA(Skip).reindex(rIndex)))) }
    )(that)(Joiner[Row](joinStrategy)(rowIndex.classTag))

  def join[L <: HList](them: L)(join: Join)(implicit folder: Frame.SeriesJoinFolder[L, Row, Col]): Frame[Row, Col] =
    them.foldLeft(this)(Frame.joinSeries)
}

case class TransposedFrame[Row, Col](frame: Frame[Col, Row]) extends Frame[Row, Col] {
  override def transpose: Frame[Col, Row] = frame
  def rowIndex: Index[Row] = frame.colIndex
  def colIndex: Index[Col] = frame.rowIndex
  def columnsAsSeries: Series[Col, UntypedColumn] = frame.rowsAsSeries
  def rowsAsSeries: Series[Row, UntypedColumn] = frame.columnsAsSeries
  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1] = TransposedFrame(frame.withRowIndex(ci))
  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col] = TransposedFrame(frame.withColIndex(ri))
}

object Frame {

  /**
   * Create an empty `Frame` with no values.
   */
  def empty[Row: ClassTag: Order, Col: ClassTag: Order]: Frame[Row, Col] =
    ColOrientedFrame[Row, Col](Index.empty[Row], Index.empty[Col], Column.empty)

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
    val columns = Column.fromArray(cols0.map { b =>
      TypedColumn(Column.fromCells(rows0.map { a => f(a, b) })): UntypedColumn
    }.toArray)
    ColOrientedFrame(Index.fromKeys(rows0: _*), Index.fromKeys(cols0: _*), columns)
  }

  /**
   * Construct a Frame whose rows are populated from some type `A`. Row
   * populators may exist for things like JSON objects or Shapeless Generic
   * types. For example,
   *
   * {{{
   * case class Person(name: String, age: Int)
   * val Alice = Person("Alice", 42)
   * val Bob = Person("Alice", 23)
   * Frame.fromGeneric(Alice, Bob)
   * }}}
   *
   * TODO: This should really take the row too (eg. `rows: (Row, A)*`).
   */
  def fromGeneric[A, Col: ClassTag](rows: A*)(implicit pop: RowPopulator[A, Int, Col]): Frame[Int, Col] =
    pop.frame(rows.zipWithIndex.foldLeft(pop.init) { case (state, (data, row)) =>
      pop.populate(state, row, data)
    })

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

  /** Implicit to help with inference of Row/Col in fromColumns/fromRows. Please ignore... */
  trait KeySeriesPair[L <: HList, Row, Col]
  object KeySeriesPair {
    import shapeless.::
    implicit def hnilKeySeriesPair[Row, Col] = new KeySeriesPair[HNil, Row, Col] {}
    implicit def hconsKeySeriesPair[A, T <: HList, Row, Col](implicit
        t: KeySeriesPair[T, Row, Col]) = new KeySeriesPair[(Col, Series[Row, A]) :: T, Row, Col] {}
  }

  /**
   * Given an HList of `(Col, Series[Row, V])` pairs, this will build a
   * `Frame[Row, Col]`, outer-joining all of the series together as the columns
   * of the frame.
   *
   * The use of `Generic.Aux` allows us to use auto-tupling (urgh) to allow
   * things like `Frame.fromColumns("a" -> seriesA, "b" -> seriesB)`, rather
   * than having to use explicit `HList`s.
   */
  def fromColumns[S, L <: HList, Col, Row](cols: S)(implicit
      gen: Generic.Aux[S, L],
      ev: KeySeriesPair[L, Row, Col],
      folder: SeriesMergeFolder[L, Row, Col],
      ctCol: ClassTag[Col], orderCol: Order[Col],
      ctRow: ClassTag[Row], orderRow: Order[Row]): Frame[Row, Col] =
    gen.to(cols).foldLeft(Frame.empty[Row, Col])(mergeSeries)

  /**
   * Given an HList of `(Row, Series[Col, V])` pairs, this will build a
   * `Frame[Row, Col]`, outer-joining all of the series together as the rows
   * of the frame.
   *
   * The use of `Generic.Aux` allows us to use auto-tupling (urgh) to allow
   * things like `Frame.fromColumns("a" -> seriesA, "b" -> seriesB)`, rather
   * than having to use explicit `HList`s.
   */
  def fromRows[S, L <: HList, Col, Row](rows: S)(implicit
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
    val extractor = sel.extractor
    val colKeys = sel.getOrElse(cols.index.keys.toList)
    for (p <- extractor.prepare(cols, colKeys)) {
      index foreach { (_, row) =>
        extractor.extract(row, p).foreach(bldr.add(_, row))
      }
    }
    bldr.result()
  }

  private def sortBy[I: ClassTag: Order, K, A: Order](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, A]): Index[I] = {
    import spire.compat._
    import scala.collection.mutable.ArrayBuffer

    val extractor = sel.extractor
    val colKeys = sel.getOrElse(cols.index.keys.toList)
    var buffer: ArrayBuffer[(Cell[A], I, Int)] = new ArrayBuffer
    for (p <- extractor.prepare(cols, colKeys)) {
      index foreach { (key, row) =>
        val sortKey = extractor.extract(row, p)
        buffer += ((sortKey, key, row))
      }
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
    val extractor = sel.extractor
    val colKeys = sel getOrElse cols.index.keys.toList
    for (p <- extractor.prepare(cols, colKeys)) {
      index foreach { (_, row) =>
        extractor.extract(row, p).foreach(bldr.add(_, row))
      }
    }
    bldr.result().sorted
  }

  private def extract[I, K, A](index: Index[I], cols: Series[K, UntypedColumn], sel: AxisSelection[K, A]): Series[I, A] = {
    val extractor = sel.extractor
    val keys = sel getOrElse cols.index.keys.toList
    val bldr = Vector.newBuilder[Cell[A]]
    extractor.prepare(cols, keys).foreach { p =>
      index foreach { (key, row) =>
        bldr += extractor.extract(row, p)
      }
    }
    Series(index.resetIndices, Column.fromCells(bldr.result()))
  }
}

case class ColOrientedFrame[Row, Col](
      rowIndex: Index[Row],
      colIndex: Index[Col],
      cols: Column[UntypedColumn])
    extends Frame[Row, Col] {

  def columnsAsSeries: Series[Col, UntypedColumn] = Series(colIndex, cols)
  def rowsAsSeries: Series[Row, UntypedColumn] = Series(rowIndex, Column[UntypedColumn]({ row =>
    new RowView(col => col, row)
  }))

  def withColIndex[C1](ci: Index[C1]): Frame[Row, C1] =
    ColOrientedFrame(rowIndex, ci, cols)

  def withRowIndex[R1](ri: Index[R1]): Frame[R1, Col] =
    ColOrientedFrame(ri, colIndex, cols)

  private final class RowView(trans: UntypedColumn => UntypedColumn, row: Int) extends UntypedColumn {
    def cast[B: ColumnTyper]: Column[B] = Column.wrap[B] { colIdx =>
      for {
        col <- cols(colIndex.indexAt(colIdx))
        value <- trans(col).cast[B].apply(row)
      } yield value
    }
    private def transform(f: UntypedColumn => UntypedColumn) = new RowView(trans andThen f, row)
    def mask(bits: Int => Boolean): UntypedColumn = transform(_.mask(bits))
    def shift(rows: Int): UntypedColumn = transform(_.shift(rows))
    def reindex(index: Array[Int]): UntypedColumn = transform(_.reindex(index))
    def setNA(na: Int): UntypedColumn = transform(_.setNA(na))
  }
}

object ColOrientedFrame {
  def apply[Row, Col](rowIdx: Index[Row], cols: Series[Col, UntypedColumn]): Frame[Row, Col] =
    ColOrientedFrame(rowIdx, cols.index, cols.column)
}
