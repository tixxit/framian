package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.Monoid
import spire.syntax.monoid._

class MonoidReducer[A: Monoid] extends Reducer[A, A] {
  type Out = Cell[A]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int, acc: A): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        loop(i + 1, acc |+| column.valueAt(row))
      } else if (column.nonValueAt(row) == NA) {
        loop(i + 1, acc)
      } else {
        NM
      }
    } else Value(acc)

    loop(start, Monoid[A].id)
  }
}
