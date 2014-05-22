package pellucid.pframe
package reduce

import scala.annotation.tailrec

import spire.algebra.Field
import spire.syntax.field._

final class Mean[A: Field] extends Reducer[A, A] {

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int, sum: A, count: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        loop(i + 1, sum + column.valueAt(row), count + 1)
      } else {
        loop(i + 1, sum, count)
      }
    } else if (count > 0) {
      Value(sum / count)
    } else {
      NM
    }

    loop(start, Field[A].zero, 0)
  }
}
