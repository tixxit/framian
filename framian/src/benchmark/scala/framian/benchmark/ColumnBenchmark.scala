package framian.benchmark

import java.util.concurrent.TimeUnit

import scala.collection.immutable.BitSet
import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.openjdk.jmh.annotations.{ BenchmarkMode, Mode, OutputTimeUnit }

import framian.Column
import framian.column.Mask

class DenseColumnMapBenchmark {

  @Benchmark
  def squareArray(data: DoubleData) = {
    val xs = data.data
    val ys = new Array[Double](xs.length)
    var i = 0
    while (i < xs.length) {
      val x = xs(i)
      ys(i) = x * x
      i += 1
    }
    ys
  }

  @Benchmark
  def squareMaskedArray(data: DoubleData) = {
    val xs = data.data
    val ys = new Array[Double](xs.length)
    var i = 0
    while (i < xs.length) {
      if (!(data.na(i) || data.nm(i))) {
        val x = xs(i)
        ys(i) = x * x
      }
      i += 1
    }
    ys
  }

  @Benchmark
  def squareColumn(data: DoubleData) =
    data.col.map(x => x * x)

  @Benchmark
  def squareBitSetMaskedArray(data: DoubleData) = {
    val xs = data.data
    val ys = new Array[Double](xs.length)
    var i = 0
    while (i < xs.length) {
      if (!(data.na0(i) || data.nm0(i))) {
        val x = xs(i)
        ys(i) = x * x
      }
      i += 1
    }
    ys
  }
}

@State(Scope.Benchmark)
class DoubleData {
  val size = 1000
  val rng = new Random(42)
  val data: Array[Double] = Array.fill(size)(rng.nextDouble)
  val na: Mask = Data.mask(rng, size, 0.1)
  val nm: Mask = Data.mask(rng, size, 0.01)
  val col: Column[Double] = Column.dense(data, na, nm)

  val na0: BitSet = na.toBitSet
  val nm0: BitSet = nm.toBitSet
}
