package framian.benchmark

import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }

import framian.Column
import framian.column.Mask

class MapWithSmallReadBenchmark {
  import Data.work

  @Benchmark
  def dense10(data: MapData): Int =
    work(data.denseColumn.map(data.f), data.size / 10)

  @Benchmark
  def eval10(data: MapData): Int =
    work(data.evalColumn.map(data.f), data.size / 10)

  @Benchmark
  def optimisticMemoized10(data: MapData): Int =
    work(data.optMemoColumn.map(data.f), data.size / 10)

  @Benchmark
  def pessimisticMemoized10(data: MapData): Int =
    work(data.pesMemoColumn.map(data.f), data.size / 10)

  @Benchmark
  def dense1(data: MapData): Int =
    work(data.denseColumn.map(data.f), data.size / 100)

  @Benchmark
  def eval1(data: MapData): Int =
    work(data.evalColumn.map(data.f), data.size / 100)

  @Benchmark
  def optimisticMemoized1(data: MapData): Int =
    work(data.optMemoColumn.map(data.f), data.size / 100)

  @Benchmark
  def pessimisticMemoized1(data: MapData): Int =
    work(data.pesMemoColumn.map(data.f), data.size / 100)

  @Benchmark
  def dense50(data: MapData): Int =
    work(data.denseColumn.map(data.f), data.size / 2)

  @Benchmark
  def eval50(data: MapData): Int =
    work(data.evalColumn.map(data.f), data.size / 2)

  @Benchmark
  def optimisticMemoized50(data: MapData): Int =
    work(data.optMemoColumn.map(data.f), data.size / 2)

  @Benchmark
  def pessimisticMemoized50(data: MapData): Int =
    work(data.pesMemoColumn.map(data.f), data.size / 2)

  @Benchmark
  def dense0(data: MapData) =
    data.denseColumn.map(data.f)

  @Benchmark
  def eval0(data: MapData) =
    data.evalColumn.map(data.f)

  @Benchmark
  def optimisticMemoized0(data: MapData) =
    data.optMemoColumn.map(data.f)

  @Benchmark
  def pessimisticMemoized0(data: MapData) =
    data.pesMemoColumn.map(data.f)
}

class ColumnMapBenchmark {
  import Data.work

  @Benchmark
  def dense(data: MapData): Int =
    work(data.denseColumn.map(data.f), data.size)

  @Benchmark
  def eval(data: MapData): Int =
    work(data.evalColumn.map(data.f), data.size)

  @Benchmark
  def optimisticMemoized(data: MapData): Int =
    work(data.optMemoColumn.map(data.f), data.size)

  @Benchmark
  def pessimisticMemoized(data: MapData): Int =
    work(data.pesMemoColumn.map(data.f), data.size)
}

class ManyReadBenchmark {
  import Data.work

  @Benchmark
  def dense(data: MapData): Int = {
    val col = data.denseColumn.map(data.f)
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size)
  }

  @Benchmark
  def eval(data: MapData): Int = {
    val col = data.evalColumn.map(data.f)
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size)
  }

  @Benchmark
  def optimisticMemoized(data: MapData): Int = {
    val col = data.optMemoColumn.map(data.f)
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size)
  }

  @Benchmark
  def pessimisticMemoized(data: MapData): Int = {
    val col = data.pesMemoColumn.map(data.f)
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size) +
    work(col, data.size)
  }
}

class ManyMapBenchmark {
  import Data.work

  private def mult(col: Column[Int], f: Int => Int, i: Int): Column[Int] =
    if (i == 0) col else mult(col.map(f), f, i - 1)

  @Benchmark
  def dense5(data: MapData): Int = work(mult(data.denseColumn, data.f, 5), data.size)

  @Benchmark
  def dense10(data: MapData): Int = work(mult(data.denseColumn, data.f, 10), data.size)

  @Benchmark
  def dense20(data: MapData): Int = work(mult(data.denseColumn, data.f, 20), data.size)

  @Benchmark
  def dense40(data: MapData): Int = work(mult(data.denseColumn, data.f, 40), data.size)

  @Benchmark
  def eval5(data: MapData): Int = work(mult(data.evalColumn, data.f, 5), data.size)

  @Benchmark
  def eval10(data: MapData): Int = work(mult(data.evalColumn, data.f, 10), data.size)

  @Benchmark
  def eval20(data: MapData): Int = work(mult(data.evalColumn, data.f, 20), data.size)

  @Benchmark
  def eval40(data: MapData): Int = work(mult(data.evalColumn, data.f, 40), data.size)
}

@State(Scope.Benchmark)
class MapData extends Data {
  val f: Int => Int = { x => x + 1 }

  val denseColumn: Column[Int] = Column.dense(Array.range(0, size))
  val evalColumn: Column[Int] = Column.eval(row => framian.Value(row))
  val optMemoColumn: Column[Int] = evalColumn.memoize(true)
  val pesMemoColumn: Column[Int] = evalColumn.memoize(false)
}
