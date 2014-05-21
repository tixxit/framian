package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.{ Semigroup, Monoid }
import spire.syntax.semigroup._

class SemigroupReducer[A: Semigroup] extends Reducer[A, A] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop0(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        loop1(i + 1, column.value(row))
      } else if (column.missing(row) == NA) {
        loop0(i + 1)
      } else {
        NM
      }
    } else NA

    @tailrec def loop1(i: Int, acc: A): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        val value = column.value(row)
        loop1(i + 1, acc |+| value)
      } else if (column.missing(row) == NA) {
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
