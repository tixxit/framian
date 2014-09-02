package framian.benchmark

import scala.collection.immutable.BitSet
import scala.util.Random

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }

import framian.column.{ Column, Mask }

class ColumnBenchmark {

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
}

//class MaskBenchmark {
//
//  @Benchmark
//  def loopThroughMask(data: DoubleData) = {
//    val na = data.na
//    var i = 0
//    var cnt = 0
//    while (i < na.size) {
//      if (na(i)) cnt += 1
//      i += 1
//    }
//    cnt
//  }
//}

@State(Scope.Benchmark)
class DoubleData {
  val size = 1000
  val rng = new Random(42)
  val data: Array[Double] = Array.fill(size)(rng.nextDouble)
  val na: Mask = Data.mask(rng, size)
  val nm: Mask = Data.mask(rng, size, 0.01)
  val col: Column[Double] = Column.dense(data, na, nm)
}

object Data {
  def mask(rng: Random, n: Int, p: Double = 0.1): Mask =
    Mask(Seq.fill(1000)(rng.nextDouble).zipWithIndex.filter(_._1 < p).map(_._2): _*)
}
