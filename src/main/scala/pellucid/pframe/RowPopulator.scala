package pellucid.pframe

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

trait RowPopulatorLow0 {
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

object RowPopulator extends RowPopulatorLow0 {
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
