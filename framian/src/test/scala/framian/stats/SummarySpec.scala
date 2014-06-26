package framian
package stats

import org.specs2.mutable._

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._

import framian.stats.ops._

class SummarySpec extends Specification {
  "summary from Frame" should {
    "be empty for empty frame" in {
      summary(Frame.empty[Int, Int]) must_== Frame.empty[Int, Int]
    }

    "be NA for empty col" in {
      val emptyFrame = Frame.fromSeries(
        "x" -> Series.fromCells[Int, Double](0 -> NA, 1 -> NA, 2 -> NA)
      )
      val expected = Frame.fromSeries(
        "x" -> Series.fromCells[String, Double](
          summary.Mean -> NA,
          summary.Median -> NA,
          summary.Max -> NA,
          summary.Min -> NA
        )
      ).transpose
      summary(emptyFrame) must_== expected
    }
  }
}
