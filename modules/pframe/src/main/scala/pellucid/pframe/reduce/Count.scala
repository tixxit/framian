package pellucid.pframe
package reduce

import scala.annotation.tailrec

final object Count extends Reducer[Any, Int] {

  def reduce(column: Column[Any], indices: Array[Int], start: Int, end: Int): Value[Int] = {
    @tailrec def count(i: Int, n: Int): Int = if (i < end) {
      val m = if (column.isValueAt(indices(i))) n + 1 else n
      count(i + 1, m)
    } else n

    Value(count(start, 0))
  }
}
