package framian
package benchmark

import scala.util.Random

trait Data {
  val size: Int = 1000
  val rng: Random = new Random(42)
}

object Data {
  final def work(col: Column[Int], size: Int): Int = {
    var sum = 0
    var i = 0
    while (i < size) {
      sum -= col(i).getOrElse(0)
      i += 1
    }
    sum
  }
}

