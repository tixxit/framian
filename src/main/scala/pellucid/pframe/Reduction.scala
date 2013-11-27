package pellucid.pframe

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

/**
 * A low level trait for implementing reductions.
 */
trait Reducer[-A, +B] {
  def reduce(column: Column[A], indices: Array[Int], start: Int, end: Int): B
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

