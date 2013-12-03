package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.Order
import spire.syntax.order._

private[reduce] final class Max[A: Order] extends Reducer[A, Option[A]] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Option[A] = {
    @tailrec def loop0(i: Int): Option[A] = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        Some(loop1(i + 1, column.value(row)))
      } else {
        loop0(i + 1)
      }
    } else None

    @tailrec def loop1(i: Int, max: A): A = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        val value = column.value(row)
        loop1(i + 1, if (value > max) value else max)
      } else {
        loop1(i + 1, max)
      }
    } else max

    loop0(start)
  }
}
