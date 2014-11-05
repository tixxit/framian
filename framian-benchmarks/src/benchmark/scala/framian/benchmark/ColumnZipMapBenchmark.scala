package framian.benchmark

import java.util.concurrent.TimeUnit

import scala.collection.immutable.BitSet
import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.openjdk.jmh.annotations.{ BenchmarkMode, Mode, OutputTimeUnit }

import framian.Column
import framian.column.Mask

class DenseColumnZipMapBenchmark {

  @Benchmark
  def zipMapArray(data: DenseZipMapData) = {
    val xs = data.data0
    val ys = data.data1
    val len = spire.math.min(xs.length, ys.length)
    val zs = new Array[Double](len)
    var i = 0
    while (i < xs.length && i < ys.length) {
      zs(i) = xs(i) * ys(i)
      i += 1
    }
    zs
  }

  @Benchmark
  def zipMappedMaskedArray(data: DenseZipMapData) = {
    val xs = data.data0
    val ys = data.data1
    val len = spire.math.min(xs.length, ys.length)
    val na = data.na0 | data.na1
    val nm = (data.nm0 | data.nm1).filter(i => i < len && !na(i))
    val zs = new Array[Double](len)
    var i = 0
    while (i < xs.length && i < ys.length) {
      if (!(na(i) || nm(i)))
        zs(i) = xs(i) * ys(i)
      i += 1
    }
    zs
  }

  @Benchmark
  def squareColumn(data: DenseZipMapData) =
    data.col0.zipMap(data.col1)(_ * _)
}

@State(Scope.Benchmark)
class DenseZipMapData {
  val size = 1000
  val rng = new Random(42)

  val data0: Array[Double] = Array.fill(size)(rng.nextDouble)
  val na0: Mask = Data.mask(rng, size, 0.1)
  val nm0: Mask = Data.mask(rng, size, 0.01)
  val col0: Column[Double] = Column.dense(data0, na0, nm0)

  val data1: Array[Double] = Array.fill(size)(rng.nextDouble)
  val na1: Mask = Data.mask(rng, size, 0.1)
  val nm1: Mask = Data.mask(rng, size, 0.01)
  val col1: Column[Double] = Column.dense(data1, na1, nm1)
}
