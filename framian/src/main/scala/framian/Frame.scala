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

import scala.reflect.ClassTag

import org.joda.time._

import spire.algebra._
import spire.implicits._
import spire.compat._
import shapeless.{HList, HNil, Typeable, Poly2, LUBConstraint, UnaryTCConstraint}
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

  def apply[T: ColumnTyper](r: Row, c: Col): Cell[T] = columns(c).get[T](r)
  def column[T: ColumnTyper](c: Col): Series[Row, T] =
    Series(rowIndex, columnsAsSeries(c) map (_.cast[T]) getOrElse Column.empty[T])

  def columnsAsSeries: Series[Col, UntypedColumn]
  def rowsAsSeries: Series[Row, UntypedColumn]

  def columns: ColumnSelector[Row, Col] = ColumnSelector(this)

  def isEmpty =
    columnsAsSeries.iterator.collectFirst {
      case (id, Value(column)) if Series(rowIndex, column.cast[Any]).hasValues => id
    }.isEmpty

  def ++(that: Frame[Row, Col]): Frame[Row, Col] = {
    // TODO: clean this up... really awful...
    def magicConversion[T](classTag: ClassTag[T])(col: Column[Any]): TypedColumn[T] =
      TypedColumn[T](col.asInstanceOf[Column[T]])(classTag)

    val colIds = colIndex.map(_._1).toList
    val thisCols = columnsAsSeries
    val thatColIds = colIndex.map(_._1).toList

    var rowIdx = this.rowIndex
    val colSeries = colIds.map { colId =>
      val thisColAsColumn = thisCols(colId).value.get.asInstanceOf[TypedColumn[_]]
      val newCol =
        if (thatColIds.contains(colId)) {
          val col = this.column[Any](colId) ++ that.column[Any](colId)
          rowIdx = col.index
          col
        } else
          column[Any](colId)

      (colId, magicConversion(thisColAsColumn.classTagA)(newCol.column))
    }
    Frame(rowIdx, colSeries: _*)
  }

  /** The following methods allow a user to apply reducers directly across a frame. In
    * particular, this API demands that we specify the type that the reducer accepts and
    * it will only apply it in the case that there exists a type conversion for a given
    * column.
    */
  def reduceFrame[V: ClassTag: ColumnTyper, R: ClassTag: ColumnTyper](reducer: Reducer[V, R]): Series[Col, R] =
    Series.fromCells(columnsAsSeries.denseIterator.map { case (key, col) =>
      (key, Series(rowIndex, col.cast[V]).reduce(reducer))
    })

  def reduceFrameByKey[V: ClassTag: ColumnTyper, R: ClassTag: ColumnTyper](reducer: Reducer[V, R]): Frame[Row, Col] =
    Frame.fromSeries(columnsAsSeries.denseIterator.map { case (key, col) =>
      (key, Series(rowIndex, col.cast[V]).reduceByKey(reducer))
    }.toSeq: _*)

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
    val columns = columnsAsSeries.denseIterator.toSeq
    object grouper extends Index.Grouper[Row] {
      case class State(rows: Int, keys: Vector[Array[R1]], cols: Series[C1, UntypedColumn]) {
        def result(): Frame[R1, C1] = Frame(
          Index(Array.concat(keys: _*)),
          cols.denseIterator.toSeq: _*)
      }

      def init = State(0, Vector.empty, Series.empty)
      def group(state: State)(keys: Array[Row], indices: Array[Int], start: Int, end: Int): State = {
        val groupRowIndex = Index(keys.slice(start, end), indices.slice(start, end))
        val groupKey = keys(start)
        val group = Frame(groupRowIndex, columns: _*)

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
  def addColumn[T: ClassTag](column: Seq[T], columnKey: Col): Frame[Row, Col] =
    this.merge(Series(rowIndex, Column.fromArray(column.toArray)), columnKey)(Merge.Outer)

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

  def merge[T: ClassTag: ColumnTyper](that: Series[Row, T], columnKey: Col)(mergeStrategy: Merge): Frame[Row, Col] =
    genericJoin[Series[Row, T]](
      { series: Series[Row, T] => series.index },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => series: Series[Row, T] =>
        Seq((columnKey, TypedColumn(series.column.setNA(Skip).reindex(rIndex)))) }
    )(that)(Merger[Row](mergeStrategy)(rowIndex.classTag))

  def join(that: Frame[Row, Col])(joinStrategy: Join): Frame[Row, Col] =
    genericJoin[Frame[Row, Col]](
      { frame: Frame[Row, Col] => frame.rowIndex },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => frame: Frame[Row, Col] =>
        frame.columnsAsSeries.denseIterator.map { case (key, col) =>
          (key, col.setNA(Skip).reindex(rIndex))
        } .toSeq }
    )(that)(Joiner[Row](joinStrategy)(rowIndex.classTag))

  def join[T: ClassTag: ColumnTyper](that: Series[Row, T], columnKey: Col)(joinStrategy: Join): Frame[Row, Col] =
    genericJoin[Series[Row, T]](
      { series: Series[Row, T] => series.index },
      { (keys: Array[Row], lIndex: Array[Int], rIndex: Array[Int]) => series: Series[Row, T] =>
        Seq((columnKey, TypedColumn(series.column.setNA(Skip).reindex(rIndex)))) }
    )(that)(Joiner[Row](joinStrategy)(rowIndex.classTag))

  import LUBConstraint.<<:
  import Frame.joinSeries
  def join[TSeries <: HList: <<:[Series[Row, _]]#λ]
          (them: TSeries, columnKeys: Seq[Col] = Seq())
          (join: Join)
          (implicit
             tf1: LeftFolder.Aux[TSeries, (List[Col], Frame[Row, Col]), joinSeries.type, (List[Col], Frame[Row, Col])],
             tpe: Typeable[Index[Col]]
          ): Frame[Row, Col] = {
    val columnIndices = (columnKeys.isEmpty, colIndex.cast[Index[Int]]) match {
      case (false, _) =>
        columnKeys
      case (_, Some(_)) if colIndex.isEmpty =>
        0 to (them.runtimeLength - 1)
      case (_, Some(index)) =>
        val colMax = index.max._1
        (colMax + 1) to (colMax + them.runtimeLength)
      case _ =>
        throw new Exception("Cannot create default column index values if column type is not numeric.")
    }

    them.foldLeft(columnIndices.toList.asInstanceOf[List[Col]], this)(joinSeries)._2
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

object Frame {

  object joinSeries extends Poly2 {
    implicit def caseT[T: ClassTag: ColumnTyper, Row: ClassTag: Order: ColumnTyper, Col: ClassTag: Order: ColumnTyper] =
      at[(List[Col], Frame[Row, Col]), Series[Row, T]] {
        case ((columnIndex :: columnIndices, frame), series) =>
          (columnIndices, frame.join(series, columnIndex)(Join.Outer))
      }
  }

  def empty[Row: ClassTag: Order: ColumnTyper, Col: ClassTag: Order: ColumnTyper]: Frame[Row, Col] =
    ColOrientedFrame[Row, Col](Index.empty, Index.empty, Column.empty)

  def apply[Row: ClassTag: Order: ColumnTyper, Col: ClassTag: Order: ColumnTyper](
    rowIndex: Index[Row],
    colPairs: (Col,UntypedColumn)*
  ): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    ColOrientedFrame(rowIndex, Index(colKeys.toArray), Column.fromArray(cols.toArray))
  }

  def fill[A: Order: ClassTag, B: Order: ClassTag, C: ClassTag](rows: Iterable[A], cols: Iterable[B])(f: (A, B) => Cell[C]): Frame[A, B] = {
    val rows0 = rows.toVector
    val cols0 = cols.toVector
    val columns = Column.fromArray(cols0.map { b =>
      TypedColumn(Column.fromCells(rows0.map { a => f(a, b) })): UntypedColumn
    }.toArray)
    fromColumns(Index.fromKeys(rows0: _*), Index.fromKeys(cols0: _*), columns)
  }

  def fromRows[A, Col: ClassTag](rows: A*)(implicit pop: RowPopulator[A, Int, Col]): Frame[Int, Col] =
    pop.frame(rows.zipWithIndex.foldLeft(pop.init) { case (state, (data, row)) =>
      pop.populate(state, row, data)
    })

  def fromColumns[Row, Col](
    rowIdx: Index[Row],
    colIdx: Index[Col],
    cols: Column[UntypedColumn]
  ): Frame[Row, Col] =
    ColOrientedFrame(rowIdx, colIdx, cols)

  def fromSeries[Row: ClassTag: Order: ColumnTyper, Col: ClassTag: Order: ColumnTyper, Value: ClassTag: ColumnTyper](
    cols: (Col, Series[Row, Value])*
  ): Frame[Row,Col] =
    cols.foldLeft[Frame[Row, Col]](Frame.empty[Row, Col]) {
      case (accum, (id, series)) => accum.join(series, id)(Join.Outer)
    }

  import LUBConstraint.<<:
  def fromHList[Row: ClassTag: Order: ColumnTyper, Col: ClassTag: Order: ColumnTyper, TSeries <: HList: <<:[Series[Row, _]]#λ]
               (colIndex: Index[Col], colSeries: TSeries)
               (implicit
                  tf: LeftFolder.Aux[TSeries, (List[Col], Frame[Row,Col]), joinSeries.type, (List[Col], Frame[Row,Col])]
               ): Frame[Row,Col] =
    Frame.empty[Row, Col].join(colSeries, colIndex.keys.toSeq)(Join.Outer)

  def fromHList[Row: ClassTag: Order: ColumnTyper, TSeries <: HList: <<:[Series[Row, _]]#λ]
               (colSeries: TSeries)
               (implicit
                  tf: LeftFolder.Aux[TSeries,(List[Int], Frame[Row,Int]), joinSeries.type, (List[Int], Frame[Row,Int])]
               ): Frame[Row,Int] =
    fromHList(Index.empty[Int], colSeries)

  // def concat[Row, Col](frames: Frame[Row, Col]): Frame[Row, Col]
}
