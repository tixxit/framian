package pellucid.pframe

import org.specs2.mutable._
import org.specs2.specification.{ Fragments, Fragment, Example} 
import org.specs2.matcher.Parameters
import org.specs2.ScalaCheck

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import spire.algebra._
import spire.math.Rational
import spire.laws._
import spire.std.int._
import spire.syntax.eq._

class ColumnAlgebrasSpec extends Specification with ScalaCheck /* extends Properties("ColumnAlgebras")*/ {
  import ColumnGenerators._

  // We use a pretty sketchy notion of equality here. Basically, pretending that
  // only real values matter. Also, actually only checking rows 0-1000 is bad too.
  implicit def ColumnEq[A: Eq] = new Eq[Column[A]] {
    def eqv(lhs: Column[A], rhs: Column[A]): Boolean = (0 to 1000) forall { row =>
      (lhs(row), rhs(row)) match {
        case (Value(x), Value(y)) => x === y
        case _ => true
      }
    }
  }

  implicit def arbColumn[A: Arbitrary] =
    Arbitrary(genSparseColumn(arbitrary[A]))

  def genRational: Gen[Rational] = for {
    n <- arbitrary[Long] map {
      case Long.MinValue => Long.MinValue + 1
      case n => n
    }
    d <- arbitrary[Long] map {
      case 0L => 1L
      case Long.MinValue => Long.MinValue + 1
      case n => n
    }
  } yield Rational(n, d)

  implicit def arbRational = Arbitrary(genRational)

  def checkAll(props: Properties): Fragments = {
    val examples: Seq[Example] = for {
      (name, prop) <- props.properties
    } yield {
      name ! check(prop)(Parameters(maxDiscardRatio = 20F))
    }

    Fragments.createList(examples: _*)
  }

  "ColumnAlgebras" should {
    checkAll(RingLaws[Column[Int]].ring)
    checkAll(RingLaws[Column[Rational]].field)
  }
}
