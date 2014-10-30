package framian
package stats

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._

import shapeless._

import framian.stats.ops._

class SummarySpec extends Specification {
  /*
  lazy val stats = List(Summary.Mean, Summary.Median, Summary.Max, Summary.Min)
  lazy val statsIndex = Index.fromKeys(stats: _*)

  "summary from Frame" should {

    "be empty for empty frame" in {
      summary(Frame.empty[Int, Int]) must_==
        Frame.empty[Int, Int].withColIndex(statsIndex)
    }

    "be NA for empty col" in {
      val emptyFrame = Series.fromCells[Int, Double](0 -> NA, 1 -> NA, 2 -> NA).toFrame("x")
      val expected = Frame.fill[String, String, Double](List("x"), stats) { (_, _) => NA }
      summary(emptyFrame) must_== expected
    }

    "summarize dense frame" in {
      val input = Frame.fromRows(
        1 :: 2 :: HNil,
        3 :: 3 :: HNil,
        2 :: 4 :: HNil
      )

      val expected = Frame.fromRows(
        2 :: 2 :: 3 :: 1 :: HNil,
        3 :: 3 :: 4 :: 2 :: HNil
      ).withColIndex(statsIndex)

      input.summary must_== expected
    }

    "summarize sparse frame" in {
      val input = Frame.mergeColumns(
        0 -> Series.fromCells[Int, Int](0 -> NA, 1 -> Value(3), 2 -> Value(2), 3 -> NA, 4 -> Value(1)),
        1 -> Series.fromCells[Int, Int](0 -> Value(2), 1 -> NM, 2 -> NM, 3 -> Value(4), 4 -> Value(3))
      )

      val expected = Frame.fill(List(0, 1), stats) {
        case (0, summary.Mean) => Value(2)
        case (0, summary.Median) => Value(2)
        case (0, summary.Max) => Value(3)
        case (0, summary.Min) => Value(1)
        case (1, summary.Mean) => NM
        case (1, summary.Median) => NM
        case (1, summary.Max) => NM
        case (1, summary.Min) => NM
      }

      input.summary must_== expected
    }
  }

  "summary for Series" should {
    "NAs for empty series" in {
      Series.empty[Int, Double].summary must_==
        Series.fromCells(stats.map(_ -> NA): _*)
    }

    "summarize dense series" in {
      summary(Series(0 -> 1D, 1 -> 3D, 2 -> 2D)) must_==
        Series(
          summary.Mean -> 2D,
          summary.Median -> 2D,
          summary.Max -> 3D,
          summary.Min -> 1D)
    }

    "summarize series with NAs" in {
      summary(Series.fromCells(0 -> Value(1D), 1 -> NA, 2 -> Value(3D), 3 -> NA, 4 -> Value(2D), 5 -> NA)) must_==
        Series(
          summary.Mean -> 2D,
          summary.Median -> 2D,
          summary.Max -> 3D,
          summary.Min -> 1D)
    }

    "summarize series with NMs" in {
      summary(Series.fromCells(0 -> Value(1D), 1 -> NM, 2 -> Value(3D), 3 -> NA, 4 -> Value(2D), 5 -> NA)) must_==
        Series.fromCells[String, Double](
          summary.Mean -> NM,
          summary.Median -> NM,
          summary.Max -> NM,
          summary.Min -> NM)
    }
  }
  */
}
