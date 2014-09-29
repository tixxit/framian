package framian.benchmark

import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }

import framian.Column
import framian.column.Mask

//class ColumnFilterBenchmark {
//  import Data.work
//
//  @Benchmark
//  def dense(data: FilterData): Int =
//    work(data.denseColumn.filter(data.p), data.size)
//
//  @Benchmark
//  def eval(data: FilterData): Int =
//    work(data.evalColumn.filter(data.p), data.size)
//
//  @Benchmark
//  def optimisticMemoized(data: FilterData): Int =
//    work(data.optMemoColumn.filter(data.p), data.size)
//
//  @Benchmark
//  def pessimisticMemoized(data: FilterData): Int =
//    work(data.pesMemoColumn.filter(data.p), data.size)
//}

@State(Scope.Benchmark)
class FilterData extends Data {
  val p: Int => Boolean = { x => x % 2 == 0 }

  val denseColumn: Column[Int] = Column.dense(Array.range(0, size))
  val evalColumn: Column[Int] = Column.eval(row => framian.Value(row))
  val optMemoColumn: Column[Int] = evalColumn.memoize(true)
  val pesMemoColumn: Column[Int] = evalColumn.memoize(false)
}
