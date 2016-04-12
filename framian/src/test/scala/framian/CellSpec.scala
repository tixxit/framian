package framian

import spire.algebra.{ Eq, Order, Semigroup, Monoid }
import spire.syntax.order._
import spire.syntax.semigroup._
import spire.implicits._

class CellSpec extends FramianSpec {
  "Cell" should {
    "be constructable from Option" in {
      Cell.fromOption(Some(2)) should === (Value(2))
      Cell.fromOption(None) should === (NA)
    }

    "have sane comparison" in {
      val order = Cell.cellOrder[Int]
      order.compare(Value(1), Value(1)) should === (0)
      order.compare(Value(2), Value(1)) should === (1)
      order.compare(Value(1), Value(2)) should === (-1)
      order.compare(NA, NA) should === (0)
      order.compare(NM, NM) should === (0)
      order.compare(NA, NM) should === (-1)
      order.compare(NA, Value(1)) should === (-1)
      order.compare(Value(1), NA) should === (1)
      order.compare(NM, NA) should === (1)
      order.compare(Value(1), NM) should === (1)
    }

    "have sane equality" in {
      NA shouldBe NA
      NA shouldBe Value(NA)
      Value(NA) shouldBe NA
      NM shouldBe NM
      NM shouldBe Value(NM)
      Value(NM) shouldBe NM
      Value(2) shouldBe Value(2)
    }

    "fold correctly" in {
      Value(1).fold(???, ???)(x => x + 1) should === (2)
      NA.fold(42, ???)(x => ???) should === (42)
      NM.fold(???, 42)(x => ???) should === (42)
    }

    "return value for getOrElse when Value" in {
      Value(42) getOrElse ??? should === (42)
    }

    "return default for getOrElse when NA/NM" in {
      NA getOrElse 42 should === (42)
      NM getOrElse 42 should === (42)
    }

    "be mappable" in {
      Value(7) map (_ * 3) should === (Value(21))
      (NA: Cell[Int]) map (_ * 3) should === (NA)
      (NM: Cell[Int]) map (_ * 3) should === (NM)
    }

    "be flatMappable" in {
      Value(7) flatMap (_ => NA) should === (NA)
      Value(7) flatMap (x => Value(x * 3)) should === (Value(21))
      NA flatMap (_ => NM) should === (NA)
      NM flatMap (_ => NA) should === (NM)
    }

    "filter changes cell to NA when false" in {
      Value(2) filter (_ == 2) should === (Value(2))
      Value(2) filter (_ != 2) should === (NA)
      NA filter (_ == 2) should === (NA)
      NM filter (_ == 2) should === (NM)
    }

    "zipMap values" in {
      Value(1).zipMap(Value(3D))(_ + _) should === (Value(4D))
    }

    "zipMap NAs" in {
      (NA: Cell[Int]).zipMap(NA: Cell[Int])(_ + _) should === (NA)

      Value(2).zipMap(NA: Cell[Int])(_ + _) should === (NA)
      (NA: Cell[Int]).zipMap(Value(2))(_ + _) should === (NA)

      (NA: Cell[Int]).zipMap(NM: Cell[Int])(_ + _) should === (NA)
      (NM: Cell[Int]).zipMap(NA: Cell[Int])(_ + _) should === (NA)
    }

    "zipMap NMs" in {
      (NM: Cell[Int]).zipMap(NM: Cell[Int])(_ + _) should === (NM)

      Value(2).zipMap(NM: Cell[Int])(_ + _) should === (NM)
      (NM: Cell[Int]).zipMap(Value(2))(_ + _) should === (NM)
    }
  }
}
