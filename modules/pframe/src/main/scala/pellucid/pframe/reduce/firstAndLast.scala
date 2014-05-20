package pellucid.pframe
package reduce

import scala.annotation.tailrec

final class First[A] extends Reducer[A, A] {
  type Out = Cell[A]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int): Cell[A] = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        Value(column.value(row))
      } else if (column.missing(row) == NA) {
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
      if (column.exists(row)) {
        Value(column.value(row))
      } else if (column.missing(row) == NA) {
        loop(i - 1)
      } else {
        NM
      }
    } else NA

    loop(end - 1)
  }
}


final class FirstN[A](n: Int) extends Reducer[A, List[A]] {
  require(n > 0, s"new FirstN(n = $n), but n must be greater than 0")
  type Out = Cell[List[A]]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Out = {
    val rows = List.newBuilder[A]

    @tailrec def loop(i: Int, k: Int): Out = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        rows += column.value(row)
        if (k == n) Value(rows.result())
        else loop(i + 1, k + 1)
      } else if (column.missing(row) == NA) {
        loop(i + 1, k)
      } else {
        NM
      }
    } else NA

    loop(start, 1)
  }
}


final class LastN[A](n: Int) extends Reducer[A, List[A]] {
  require(n > 0, s"new LastN(n = $n), but n must be greater than 0")
  type Out = Cell[List[A]]

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): Out = {
    var rows = List.empty[A]

    @tailrec def loop(i: Int, k: Int): Out = if (i >= start) {
      val row = indices(i)
      if (column.exists(row)) {
        rows = column.value(row) :: rows
        if (k == n) Value(rows)
        else loop(i - 1, k + 1)
      } else if (column.missing(row) == NA) {
        loop(i - 1, k)
      } else {
        NM
      }
    } else NA

    loop(end - 1, 1)
  }
}
