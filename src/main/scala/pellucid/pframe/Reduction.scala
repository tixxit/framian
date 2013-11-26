package pellucid.pframe

import scala.annotation.tailrec
import scala.collection.mutable.{ ArrayBuilder, Builder }
import scala.reflect.ClassTag

import spire.algebra._

trait Reducer[A, B] {
  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): B
}

object Reducers {
  def mean[A: Field] = new Mean[A]

  def monoid[A: Monoid] = new MonoidReducer[A]
}

final class Mean[A: Field] extends Reducer[A, A] {
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

final class MonoidReducer[A: Monoid] extends Reducer[A, A] {
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

final class Reduction[K: ClassTag, A, B: ClassTag](column: Column[A], reducer: Reducer[A, B]) extends Index.Grouper[K] {
  final class State {
    val keys = ArrayBuilder.make[K]
    val values = ArrayBuilder.make[B]

    def add(key: K, value: B) {
      keys += key
      values += value
    }

    def result() = (keys.result(), values.result())
  }

  def init = new State

  def group(state: State)(keys: Array[K], indices: Array[Int], start: Int, end: Int): State = {
    state.add(keys(start), reducer.reduce(column, indices, start, end))
    state
  }
}

