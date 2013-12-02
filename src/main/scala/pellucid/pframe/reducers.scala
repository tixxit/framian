package pellucid.pframe

import scala.annotation.tailrec

import spire.algebra._

object reduce {
  // TODO: Rename.
  def MonoidReducer[A: Monoid]: Reducer[A, A] = new MonoidReducer[A]

  def Mean[A: Field]: Reducer[A, A] = new Mean[A]

  def Sum[A: AdditiveMonoid]: Reducer[A, A] = MonoidReducer(spire.algebra.Monoid.additive[A])

  final object Count extends Reducer[Any, Int] {
    def reduce(column: Column[Any], indices: Array[Int], start: Int, end: Int): Int = {
      @tailrec def count(i: Int, n: Int): Int = if (i < end) {
        val m = if (column.exists(indices(i))) n + 1 else n
        count(i + 1, m)
      } else n

      count(start, 0)
    }
  }

  def Max[A: Order]: Reducer[A, Option[A]] = new Max[A]

  def Min[A: Order]: Reducer[A, Option[A]] = Max(Order[A].reverse)
}

private final class Max[A: Order] extends Reducer[A, Option[A]] {
  import spire.syntax.order._

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

private final class Mean[A: Field] extends Reducer[A, A] {
  import spire.syntax.field._

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

private final class MonoidReducer[A: Monoid] extends Reducer[A, A] {
  import spire.syntax.monoid._

  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): A = {
    @tailrec def loop(i: Int, acc: A): A = if (i < end) {
      val row = indices(i)
      if (column.exists(row)) {
        loop(i + 1, acc |+| column.value(row))
      } else {
        loop(i + 1, acc)
      }
    } else acc

    loop(start, Monoid[A].id)
  }
}
