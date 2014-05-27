package pellucid.pframe
package reduce

import scala.reflect.ClassTag

import spire.std.int._
import spire.std.double._

import org.specs2.ScalaCheck
import org.specs2.matcher.Parameters
import org.specs2.mutable._

import org.scalacheck.{ Arbitrary, Gen, Prop }


class ReducerPropSpec extends Specification with ScalaCheck {
  import Arbitrary.arbitrary
  import Prop.{ classify, collect, forAll }


  implicit val params = Parameters(minTestsOk = 20)


  def mkSeries[A](cells: IndexedSeq[Cell[A]]) =
    Series(Index(Array.range(0, cells.length)), Column.fromCells(cells))


  def emptySeriesGen[V] =
          Gen.resize(3, Gen.containerOf[IndexedSeq, Cell[V]](Gen.const(NA)))

  def sparseSeriesGen[V : Arbitrary] =
        Gen.nonEmptyContainerOf[IndexedSeq, Cell[V]](
          Gen.frequency(1 -> NA,
                        3 -> arbitrary[V].map(Value.apply)))

  def nmSeriesGen[V : Arbitrary] =
        Gen.nonEmptyContainerOf[IndexedSeq, Cell[V]](
          Gen.frequency(1 -> NM,
                        9 -> arbitrary[V].map(Value.apply)))


  def classifySparse[A](c: TraversableOnce[Cell[A]])(prop: Prop): Prop =
    classify(c.exists(_.isNonValue), "sparse", "dense")(prop)

  def classifyMeaningful[A](c: TraversableOnce[Cell[A]])(prop: Prop): Prop =
    classify(c.exists(_.isNonValue), "meaningless", "meaningful")(prop)


  def reducingEmptySeriesMustEqNA[I, O : ClassTag](reducer: Reducer[I, O]): Prop =
    forAll (emptySeriesGen[I]) { l =>
      collect(l.size) {
        mkSeries(l).reduce(reducer) must_== NA
      }
    } .set(minTestsOk = 10)

  def reducingMeaninglessSeriesMustEqNM[I : Arbitrary, O : ClassTag](reducer: Reducer[I, O]): Prop =
    forAll (nmSeriesGen[I]) { l =>
      classifyMeaningful(l) {
        l.exists(_.isNonValue) ==> (
          mkSeries(l).reduce(reducer) must_== NM
        )
      }
    } .set(minTestsOk = 10)


  "Count" should {

    "return 0 for an empty series" in {
      forAll (emptySeriesGen[Int]) { l =>
        collect(l.length) {
          mkSeries(l).reduce(Count) must_== Value(0)
        }
      } .set(minTestsOk = 10)
    }

    "return the count for a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        Series(l:_*).reduce(Count) must_== Value(l.size)
      }
    }

    "return the count for a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          mkSeries(l).reduce(Count) must_==
            Value(l.filter(_.isValue).size)
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM[Int, Int](Count)
  }


  "First" should {

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(First[Int])

    "return the first value in a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        Series(l:_*).reduce(First[Int]) must_== Value(l.head)
      }
    }

    "return the first value in a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          l.exists(_.isValue) ==> (
            mkSeries(l).reduce(First[Int]) must_== l.filter(_.isValue).head
          )
        }
      } and
      forAll (nmSeriesGen[Int]) { l =>
        classifyMeaningful(l) {
          mkSeries(l).reduce(First[Int]) must_== l.filterNot(_ == NA).head
        }
      }
    }
  }


  "FirstN" should {

    "return NA for an empty series" in {
      forAll (Arbitrary.arbitrary[Int]) { n =>
        (n > 0) ==> reducingEmptySeriesMustEqNA(FirstN[Int](n))
      }
    }

    "return the first n values in a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        val s = Series(l:_*)
        forAll (Gen.choose(1, l.size)) { n =>
          s.reduce(FirstN[Int](n)) must_== Value(l.take(n))
        }
      }
    }

    "return the first n values in a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          val s = mkSeries(l)

          forAll (Gen.choose(1, l.size)) { n =>
            val res = s.reduce(FirstN[Int](n))

            (n <= l.filter(_.isValue).size) ==> (
              res must_== Value(l.filter(_.isValue).map(_.get).take(n))
            ) ||
            (n > l.filter(_.isValue).size) ==> (
              res must_== NA
            )
          }
        }
      } and
      forAll (nmSeriesGen[Int]) { l =>
        classifyMeaningful(l) {
          val s = mkSeries(l)

          forAll (Gen.choose(1, l.filter(_.isValue).size)) { n =>
            val res = s.reduce(FirstN[Int](n))
            val containsNM = l.filterNot(_ == NA).take(n).exists(_ == NM)

            containsNM ==> (
              res must_== NM
            ) ||
            !containsNM ==> (
              res must_== Value(l.filter(_.isValue).map(_.get).take(n))
            )
          }
        }
      }
    }
  }


  "Last" should {

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(Last[Int])

    "return the first value in a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        Series(l:_*).reduce(Last[Int]) must_== Value(l.last)
      }
    }

    "return the last value in a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          l.exists(_.isValue) ==> (
            mkSeries(l).reduce(Last[Int]) must_== l.filter(_.isValue).last
          )
        }
      } and
      forAll (nmSeriesGen[Int]) { l =>
        classifyMeaningful(l) {
          mkSeries(l).reduce(Last[Int]) must_== l.filterNot(_ == NA).last
        }
      }
    }
  }


  "LastN" should {

    "return NA for an empty series" in {
      forAll (Arbitrary.arbitrary[Int]) { n =>
        (n > 0) ==> reducingEmptySeriesMustEqNA(LastN[Int](n))
      }
    }

    "return the first n values in a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        val s = Series(l:_*)
        forAll (Gen.choose(1, l.size)) { n =>
          s.reduce(LastN[Int](n)) must_== Value(l.takeRight(n))
        }
      }
    }

    "return the first n values in a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          val s = mkSeries(l)

          forAll (Gen.choose(1, l.size)) { n =>
            val res = s.reduce(LastN[Int](n))

            (n <= l.filter(_.isValue).size) ==> (
              res must_== Value(l.filter(_.isValue).map(_.get).takeRight(n))
            ) ||
            (n > l.filter(_.isValue).size) ==> (
              res must_== NA
            )
          }
        }
      } and
      forAll (nmSeriesGen[Int]) { l =>
        classifyMeaningful(l) {
          val s = mkSeries(l)

          forAll (Gen.choose(1, l.filter(_.isValue).size)) { n =>
            val res = s.reduce(LastN[Int](n))
            val containsNM = l.filterNot(_ == NA).takeRight(n).exists(_ == NM)

            containsNM ==> (
              res must_== NM
            ) ||
            !containsNM ==> (
              res must_== Value(l.filter(_.isValue).map(_.get).takeRight(n))
            )
          }
        }
      }
    }
  }


  "Max" should {

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(Max[Int])

    "return the max value in a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        Series(l:_*).reduce(Max[Int]) must_== Value(l.max)
      }
    }

    "return the max value in a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          l.exists(_.isValue) ==> (
            mkSeries(l).reduce(Max[Int]) must_==
              Value(l.filter(_.isValue).map(_.get).max)
          )
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Max[Int])
  }


  "Mean" should {

    "return NM for an empty series" in {
      forAll (emptySeriesGen[Double]) { l =>
        collect(l.length) {
          mkSeries(l).reduce(Mean[Double]) must_== NM
        }
      } .set(minTestsOk = 10)
    }

    "return the mean value of a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Double])) { l =>
        Series(l:_*).reduce(Mean[Double]) must_==
          Value(l.sum / l.length)
      }
    }

    "return the mean value of a sparse series" in {
      forAll (sparseSeriesGen[Double]) { l =>
        classifySparse(l) {
          l.exists(_.isValue) ==> (
            mkSeries(l).reduce(Mean[Double]) must_== {
              val l0 = l.filter(_.isValue)
              Value(l0.map(_.get).sum / l0.length)
            }
          )
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Mean[Double])
  }


  "Monoid" should {
    import spire.algebra.Monoid

    implicit val m = Monoid.additive[Int]

    "return monoid identity for an empty series" in {
      forAll (emptySeriesGen[Int]) { l =>
        collect(l.length) {
          {
            implicit val m = Monoid.additive[Int]
            mkSeries(l).reduce(MonoidReducer[Int]) must_== Value(m.id)
          } and {
            implicit val m = Monoid.multiplicative[Int]
            mkSeries(l).reduce(MonoidReducer[Int]) must_== Value(m.id)
          }
        }
      } .set(minTestsOk = 10)
    }

    "return the monoidal reduction of a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        {
          implicit val m = Monoid.additive[Int]
          Series(l:_*).reduce(MonoidReducer[Int]) must_== Value(l.sum)
        } and {
          implicit val m = Monoid.multiplicative[Int]
          Series(l:_*).reduce(MonoidReducer[Int]) must_== Value(l.product)
        }
      }
    }

    "return the monoidal reduction of a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          mkSeries(l).reduce(MonoidReducer[Int]) must_==
            Value(l.filter(_.isValue).map(_.get).sum)
        }
      }
    }

    "return MM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(MonoidReducer[Int])
  }


  "SemiGroup" should {
    import spire.algebra.Semigroup

    implicit val g = Semigroup.additive[Int]

    "return NA for an empty series" in reducingEmptySeriesMustEqNA(SemigroupReducer[Int])

    "return the semigroup reduction of a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        {
          implicit val g = Semigroup.additive[Int]
          Series(l:_*).reduce(SemigroupReducer[Int]) must_== Value(l.sum)
        } and {
          implicit val g = Semigroup.multiplicative[Int]
          Series(l:_*).reduce(SemigroupReducer[Int]) must_== Value(l.product)
        }
      }
    }

    "return the semigroup reduction of a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          val res = mkSeries(l).reduce(SemigroupReducer[Int])
          l.exists(_.isValue) ==> (
            res must_== Value(l.filter(_.isValue).map(_.get).sum)
          ) ||
          l.forall(_ == NA) ==> (
            res must_== NA
          )
        }
      }
    }

    "return MM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(SemigroupReducer[Int])
  }


  "Unique" should {

    "return the empty set for an empty series" in {
      forAll (emptySeriesGen[Int]) { l =>
        collect(l.length) {
          mkSeries(l).reduce(Unique[Int]) must_== Value(Set.empty)
        }
      } .set(minTestsOk = 10)
    }

    "return the unique values of a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        Series(l:_*).reduce(Unique[Int]) must_== Value(l.toSet)
      }
    }

    "return the unique values of a sparse series" in {
      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          mkSeries(l).reduce(Unique[Int]) must_== Value(l.filter(_.isValue).map(_.get).toSet)
        }
      }
    }

    "return NM if the series contains NM" in reducingMeaninglessSeriesMustEqNM(Unique[Int])
  }


  "Exists" should {

    "return false for an empty series" in {
      forAll (emptySeriesGen[Int]) { l =>
        collect(l.length) {
          mkSeries(l).reduce(Exists[Int](_ => true)) must_== Value(false)
        }
      } .set(minTestsOk = 10)
    }

    "evaluate the predicate for a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        val s = Series(l:_*)

        (
          s.reduce(Exists[Int](_ => false)) must_== Value(false)
        ) && {
          val p = ((i: Int) => i % 10 == 0)

          classify(l.exists(p), "exists=true", "exists=false") {
            s.reduce(Exists(p)) must_== Value(l.exists(p))
          }
        }
      }
    }

    "evaluate the predicate for a sparse series" in {
      val p = ((i: Int) => i % 10 == 0)

      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          mkSeries(l).reduce(Exists(p)) must_== Value(l.filter(_.isValue).map(_.get).exists(p))
        }
      } and
      forAll (nmSeriesGen[Int]) { l =>
        classifyMeaningful(l) {
          mkSeries(l).reduce(Exists(p)) must_== Value(l.filter(_.isValue).map(_.get).exists(p))
        }
      }
    }
  }


  "ForAll" should {

    "return true for an empty series" in {
      forAll (emptySeriesGen[Int]) { l =>
        collect(l.length) {
          mkSeries(l).reduce(ForAll[Int](_ => false)) must_== Value(true)
        }
      } .set(minTestsOk = 10)
    }

    "evaluate the predicate for a dense series" in {
      forAll (Gen.nonEmptyListOf(arbitrary[Int])) { l =>
        val s = Series(l:_*)

        (
          s.reduce(ForAll[Int](_ => false)) must_== Value(false)
        ) && {
          val p = ((i: Int) => i >= 0)

          classify(l.forall(p), "forall=true", "forall=false") {
            s.reduce(ForAll(p)) must_== Value(l.forall(p))
          }
        }
      }
    }

    "evaluate the predicate for a sparse series" in {
      val p = ((i: Int) => i >= 0)

      forAll (sparseSeriesGen[Int]) { l =>
        classifySparse(l) {
          mkSeries(l).reduce(ForAll(p)) must_== Value(l.filter(_.isValue).map(_.get).forall(p))
        }
      } and
      forAll (nmSeriesGen[Int]) { l =>
        classifyMeaningful(l) {
          val res = mkSeries(l).reduce(ForAll(p))
          val containsNM = l.contains(NM)
          containsNM ==> (
            res must_== Value(false)
          ) ||
          !containsNM ==> (
            res must_== Value(l.filter(_.isValue).map(_.get).forall(p))
          )
        }
      }
    }
  }
}
