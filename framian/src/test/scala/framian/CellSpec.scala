package framian

import org.specs2.mutable._

import spire.algebra.{ Eq, Order, Semigroup, Monoid }
import spire.syntax.order._
import spire.syntax.semigroup._
import spire.implicits._

class CellSpec extends Specification {
  "Cell" should {
    "be constructable from Option" in {
      Cell.fromOption(Some(2)) must_== Value(2)
      Cell.fromOption(None) must_== NA
    }

    "have sane comparison" in {
      val order = Cell.cellOrder[Int]
      order.compare(Value(1), Value(1)) must_== 0
      order.compare(Value(2), Value(1)) must_== 1
      order.compare(Value(1), Value(2)) must_== -1
      order.compare(NA, NA) must_== 0
      order.compare(NM, NM) must_== 0
      order.compare(NA, NM) must_== -1
      order.compare(NA, Value(1)) must_== -1
      order.compare(Value(1), NA) must_== 1
      order.compare(NM, NA) must_== 1
      order.compare(Value(1), NM) must_== 1
    }

    "have sane equality" in {
      NA must_== NA
      NA must_== Value(NA)
      Value(NA) must_== NA
      NM must_== NM
      NM must_== Value(NM)
      Value(NM) must_== NM
      Value(2) must_== Value(2)
    }

    "fold correctly" in {
      Value(1).fold(???, ???)(x => x + 1) must_== 2
      NA.fold(42, ???)(x => ???) must_== 42
      NM.fold(???, 42)(x => ???) must_== 42
    }

    "return value for getOrElse when Value" in {
      Value(42) getOrElse ??? must_== 42
    }

    "return default for getOrElse when NA/NM" in {
      NA getOrElse 42 must_== 42
      NM getOrElse 42 must_== 42
    }

    "be mappable" in {
      Value(7) map (_ * 3) must_== Value(21)
      (NA: Cell[Int]) map (_ * 3) must_== NA
      (NM: Cell[Int]) map (_ * 3) must_== NM
    }

    "be flatMappable" in {
      Value(7) flatMap (_ => NA) must_== NA
      Value(7) flatMap (x => Value(x * 3)) must_== Value(21)
      NA flatMap (_ => NM) must_== NA
      NM flatMap (_ => NA) must_== NM
    }

    "filter changes cell to NA when false" in {
      Value(2) filter (_ == 2) must_== Value(2)
      Value(2) filter (_ != 2) must_== NA
      NA filter (_ == 2) must_== NA
      NM filter (_ == 2) must_== NM
    }
  }
}
