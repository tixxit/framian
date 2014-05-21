package pellucid.pframe
package reduce

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

import spire.syntax.cfor._

abstract class SimpleReducer[A: ClassTag, B] extends Reducer[A, B] {
  type Out = Cell[B]

  final def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[B] = {
    val bldr = ArrayBuilder.make[A]
    cfor(start)(_ < end, _ + 1) { i =>
      val row = indices(i)
      if (column.exists(row)) {
        bldr += column.value(row)
      } else if (column.missing(row) == NM) {
        return NM
      }
    }
    reduce(bldr.result())
  }

  def reduce(data: Array[A]): Cell[B]
}
