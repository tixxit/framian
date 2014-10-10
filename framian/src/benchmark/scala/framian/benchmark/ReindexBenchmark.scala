package framian.benchmark

import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }

import framian.Column
import framian.column.Mask

//class ColumnReindexBenchmark {
//  import Data.work
//
//  @Benchmark
//  def dense(data: ReindexData): Int =
//    work(data.denseColumn.reindex(data.indices), data.size)
//
//  @Benchmark
//  def eval(data: ReindexData): Int =
//    work(data.evalColumn.reindex(data.indices), data.size)
//
//  @Benchmark
//  def optimisticMemoized(data: ReindexData): Int =
//    work(data.optMemoColumn.reindex(data.indices), data.size)
//
//  @Benchmark
//  def pessimisticMemoized(data: ReindexData): Int =
//    work(data.pesMemoColumn.reindex(data.indices), data.size)
//}

@State(Scope.Benchmark)
class ReindexData extends Data {
  val indices: Array[Int] = rng.shuffle((0 until size).toList).toArray

  val denseColumn: Column[Int] = Column.dense(Array.range(0, size))
  val evalColumn: Column[Int] = Column.eval(row => framian.Value(row))
  val optMemoColumn: Column[Int] = evalColumn.memoize(true)
  val pesMemoColumn: Column[Int] = evalColumn.memoize(false)
}
