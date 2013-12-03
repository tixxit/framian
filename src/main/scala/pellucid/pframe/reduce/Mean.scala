package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.Field
import spire.syntax.field._

private[reduce] final class Mean[A: Field] extends Reducer[A, A] {
  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): A = {
    @tailrec def loop(i: Int, sum: A, count: Int): A = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        loop(i + 1, sum + column.value(row), count + 1)
      } else {
        loop(i + 1, sum, count)
      }
    } else sum / count

    loop(start, Field[A].zero, 0)
  }
}
