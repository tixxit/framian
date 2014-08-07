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

import spire.algebra.Order
import spire.std.int._

import shapeless._

/**
 * A trait used to generate frames from sets of rows.
 *
 * The general use of an instance of this trait will be as follows:
 *
 * {{{
 * val pop = getSomeRowPopulator
 * val rows: List[(RowKey, RowData)]
 * val frame = pop.frame(rows.foldLeft(pop.init) { case (state, (row, data)) =>
 *   pop.populate(state, row, data)
 * })
 * }}}
 */
trait RowPopulator[A, Row, Col] {
  type State
  def init: State
  def populate(state: State, row: Row, data: A): State
  def frame(state: State): Frame[Row, Col]
}

trait RowPopulatorLowPriorityImplicits {
  implicit def generic[A, B, Row, Col](implicit generic: Generic.Aux[A, B],
      pop: RowPopulator[B, Row, Col]) =
    new RowPopulator[A, Row, Col] {
      type State = pop.State
      def init: State = pop.init
      def populate(state: State, row: Row, data: A): State =
        pop.populate(state, row, generic.to(data))
      def frame(state: State): Frame[Row, Col] =
        pop.frame(state)
    }
}

object RowPopulator extends RowPopulatorLowPriorityImplicits {
  implicit def HListRowPopulator[Row: Order: ClassTag, L <: HList](
      implicit pop: HListColPopulator[L]) = new HListRowPopulator[Row, L](pop)

  final class HListRowPopulator[Row: Order: ClassTag, L <: HList](val pop: HListColPopulator[L])
      extends RowPopulator[L, Row, Int] {

    type State = List[Row] :: pop.State

    def init: State = Nil :: pop.init

    def populate(state: State, row: Row, data: L): State =
      (row :: state.head) :: pop.populate(state.tail, data)

    def frame(state: State): Frame[Row, Int] = {
      val cols = pop.columns(state.tail).toArray
      val rowIndex = Index(state.head.reverse.toArray)
      val colIndex = Index(Array.range(0, cols.size))
      Frame.fromColumns(rowIndex, colIndex, Column.fromArray(cols))
    }
  }

  trait HListColPopulator[L <: HList] {
    type State <: HList
    def init: State
    def populate(state: State, data: L): State
    def columns(state: State): List[UntypedColumn]
  }

  trait HListColPopulator0 {
    implicit object HNilColPopulator extends HListColPopulator[HNil] {
      type State = HNil
      def init: HNil = HNil
      def populate(u: HNil, data: HNil): HNil = HNil
      def columns(state: State): List[UntypedColumn] = Nil
    }
  }

  object HListColPopulator extends HListColPopulator0 {
    implicit def HConsColPopulator[H: ClassTag, T <: HList](implicit tail: HListColPopulator[T]) =
      new HConsColPopulator(tail)

    final class HConsColPopulator[H: ClassTag, T <: HList](val tail: HListColPopulator[T])
        extends HListColPopulator[H :: T] {

      type State = List[H] :: tail.State

      def init = Nil :: tail.init

      def populate(state: State, data: H :: T): State =
        (data.head :: state.head) :: tail.populate(state.tail, data.tail)

      def columns(state: State): List[UntypedColumn] = {
        val col = TypedColumn(Column.fromArray(state.head.reverse.toArray))
        col :: tail.columns(state.tail)
      }
    }
  }
}
