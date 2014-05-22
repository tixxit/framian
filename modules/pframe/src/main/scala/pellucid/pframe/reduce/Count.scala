package pellucid.pframe
package reduce

import scala.annotation.tailrec

final object Count extends Reducer[Any, Int] {

  def reduce(column: Column[Any], indices: Array[Int], start: Int, end: Int): Cell[Int] = {
    @tailrec def loop(i: Int, n: Int): Cell[Int] = if (i < end) {
      val row = indices(i)
      if (column.isValueAt(row)) {
        loop(i + 1, n + 1)
      } else if (column.nonValueAt(row) == NA) {
        loop(i + 1, n)
      } else {
        NM
      }
    } else {
      Value(n)
    }

    loop(start, 0)
  }
}
