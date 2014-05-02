package pellucid.pframe
package reduce

import scala.annotation.tailrec

import org.joda.time.LocalDate

final class Current[A] extends Reducer[(LocalDate, A), A] {
  type Out = Cell[A]

  def reduce(column: Column[(LocalDate, A)], indices: Array[Int], start: Int, end: Int): Cell[A] = {
    @tailrec def loop(i: Int, latestDate: LocalDate, latestIndex: Option[Int]): Cell[A] =
      if (i < end) {
        val row = indices(i)
        if (column.exists(row)) {
          val (nextDate, _) = column.value(row) // TODO: NA/NM values here?
          if (nextDate.isAfter(latestDate)) loop(i + 1, nextDate, Some(row))
          else loop(i + 1, latestDate, latestIndex)
        } else
          loop(i + 1, latestDate, latestIndex)
      } else
        latestIndex.fold[Cell[A]](NA) { index => column(index).map(_._2) }

    loop(start, new LocalDate(0), None)
  }
}

