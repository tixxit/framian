package pellucid.pframe

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import spire.algebra._
import spire.math.Rational
import spire.laws._
import spire.std.int._
import spire.syntax.eq._

object ColumnAlgebrasSpec extends Properties("ColumnAlgebras") {
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
    n <- arbitrary[Long] filter (_ != Long.MinValue)
    d <- arbitrary[Long] filter (_ != Long.MinValue) filter (_ != 0)
  } yield Rational(n, d)

  implicit def arbRational = Arbitrary(genRational)

  include(RingLaws[Column[Int]].ring)
  include(RingLaws[Column[Rational]].field)
}
