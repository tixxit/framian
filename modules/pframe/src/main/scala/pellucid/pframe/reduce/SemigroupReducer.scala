package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.{ Semigroup, Monoid }
import spire.syntax.semigroup._

class SemigroupReducer[A: Semigroup] extends Reducer[A, A] {
  type Out = Cell[A]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop0(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        loop1(i + 1, column.valueAt(row))
      } else if (column.nonValueAt(row) == NA) {
        loop0(i + 1)
      } else {
        NM
      }
    } else NA

    @tailrec def loop1(i: Int, acc: A): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        val value = column.valueAt(row)
        loop1(i + 1, acc |+| value)
      } else if (column.nonValueAt(row) == NA) {
        loop1(i + 1, acc)
      } else {
        NM
      }
    } else {
      Value(acc)
    }

    loop0(start)
  }
}
