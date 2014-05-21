package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.Order
import spire.syntax.order._

final class Max[A: Order] extends Reducer[A, A] {
  type Out = Cell[A]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop0(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        Value(loop1(i + 1, column.valueAt(row)))
      } else {
        loop0(i + 1)
      }
    } else NA

    @tailrec def loop1(i: Int, max: A): A = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        val value = column.valueAt(row)
        loop1(i + 1, if (value > max) value else max)
      } else {
        loop1(i + 1, max)
      }
    } else max

    loop0(start)
  }
}
