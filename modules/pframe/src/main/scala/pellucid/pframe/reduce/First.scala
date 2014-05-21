package pellucid.pframe
package reduce

import scala.annotation.tailrec

final class First[A] extends Reducer[A, A] {
  type Out = Cell[A]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        Value(column.valueAt(row))
      } else if (column.nonValueAt(row) == NA) {
        loop(i + 1)
      } else {
        NM
      }
    } else NA

    loop(start)
  }
}

final class Last[A] extends Reducer[A, A] {
  type Out = Cell[A]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int): Cell[A] = if (i >= start) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        Value(column.valueAt(row))
      } else if (column.nonValueAt(row) == NA) {
        loop(i - 1)
      } else {
        NM
      }
    } else NA

    loop(end - 1)
  }
}
