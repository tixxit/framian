package framian.benchmark

import java.util.concurrent.TimeUnit

import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }

import spire.implicits._

import framian._
import framian.column.Mask

class SeriesZipMapBenchmark {
  import SeriesZipMapBenchmark.Data

  @Benchmark
  def joinIndices(data: Data) = {
    val joiner = Joiner[Int](Join.Inner)
    Index.cogroup(data.idx0, data.idx1)(joiner).result()
  }

  @Benchmark
  def reindexColumns(data: Data) = {
    val col0 = data.col0.reindex(data.indices0)
    val col1 = data.col1.reindex(data.indices1)
    (col0, col1)
  }

  @Benchmark
  def zipMapColumn(data: Data) =
    data.col0.zipMap(data.col1)(_ * _)

  @Benchmark
  def makeIndex(data: Data) =
    Index.ordered(data.indices0)

  @Benchmark
  def zipMapSeries(data: Data) =
    data.series0.zipMap(data.series1)(_ * _)
}

object SeriesZipMapBenchmark {

  @State(Scope.Benchmark)
  class Data {
    val size = 1000
    val rng = new Random(42)

    val indices0 = Array.range(0, size)
    val indices1 = Array.range(0, size)

    val data0: Array[Double] = Array.fill(size)(rng.nextDouble)
    val na0: Mask = Data.mask(rng, size, 0.1)
    val nm0: Mask = Data.mask(rng, size, 0.01)
    val col0: Column[Double] = Column.dense(data0, na0, nm0)
    val idx0: Index[Int] = Index(indices0)
    val series0: Series[Int, Double] = Series(idx0, col0)

    val data1: Array[Double] = Array.fill(size)(rng.nextDouble)
    val na1: Mask = Data.mask(rng, size, 0.1)
    val nm1: Mask = Data.mask(rng, size, 0.01)
    val col1: Column[Double] = Column.dense(data1, na1, nm1)
    val idx1: Index[Int] = Index(indices1)
    val series1: Series[Int, Double] = Series(Index.fromKeys(1 to size: _*), col1)
  }
}
