package pellucid.pframe

import org.joda.time.LocalDate

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire.algebra._

package object reduce {
  def MonoidReducer[A: Monoid]: Reducer[A, A] = new MonoidReducer[A]

  def SemigroupReducer[A: Semigroup]: Reducer[A, A] = new SemigroupReducer[A]

  def Mean[A: Field]: Reducer[A, A] = new Mean[A]

  def Sum[A: AdditiveMonoid]: Reducer[A, A] = MonoidReducer(spire.algebra.Monoid.additive[A])

  def Median[A: Field: Order: ClassTag]: Reducer[A, A] = new Median

  def Quantile[A: Field: Order: ClassTag](probabilities: Seq[Double] = Seq(0, .25, .5, .75, 1)): Reducer[A, Seq[(Double, A)]] =
    new Quantile(probabilities)

  def Outliers[A: Field: Order: ClassTag](k: Double = 1.5): Reducer[A, (Option[A], Option[A])] =
    new Outliers(k)

  def Max[A: Order]: Reducer[A, A] = new Max[A]

  def Min[A: Order]: Reducer[A, A] = Max(Order[A].reverse)

  def First[A]: Reducer[A, A] = new First[A]

  def FirstN[A](n: Int): Reducer[A, List[A]] = new FirstN[A](n)

  def Last[A]: Reducer[A, A] = new Last[A]

  def LastN[A](n: Int): Reducer[A, List[A]] = new LastN[A](n)

  def Current[A]: Reducer[(LocalDate, A), A] = new Current[A]

  def Unique[A]: Reducer[A, Set[A]] = new Unique[A]


  /** Returns a [[Reducer]] that exististentially quantifies a predicate
    * as a reduction over a collection of [[Cell]]s.
    *
    * This reducer will return true upon encountering the first value
    * that is available and meaningful ''and'' applying the predicate
    * `p` to that value returns true. Otherwise, returns false.
    *
    * This reducer is unusual in that it will ignore [[NonValue]]s, in
    * particular, it will not propogate [[NM]]. If the predicate can be
    * satisfied by a value that is available and meaningful elsewhere in
    * the collection, then this reduction should still return true.
    *
    * This reducer will only traverse the entire collection if it never
    * encounters an available and meaningful value that satisfies the
    * predicate `p`.
    *
    * @example {{{
    * Series.empty[Int, Int].reduce(Exists[Int](i => true)) == Value(false)
    *
    * Series(1 -> 1, 2 -> 2).reduce(Exists[Int](i => i < 2)) == Value(true)
    *
    * Series.fromCells[Int, Int](1 -> NA, 2 -> 1).reduce(Exists[Int](i => i < 2)) == Value(true)
    * Series.fromCells[Int, Int](1 -> NM, 2 -> 1).reduce(Exists[Int](i => i < 2)) == Value(true)
    * }}}
    *
    * @tparam  A  the value type of the column to reduce.
    * @param  p  the predicate to apply to the values of the column.
    * @return a [[Reducer]] that exististentially quantifies a predicate.
    * @see [[ForAll]]
    */
  def Exists[A](p: A => Boolean): Reducer[A, Boolean] = new Exists(p)


  /** Returns a [[Reducer]] that universally quantifies a predicate
    * as a reduction over a collection of [[Cell]]s.
    *
    * This reducer will return false upon encountering the first value
    * that is not meaningful, or the first value that is available and
    * meaningful ''and'' applying the predicate `p` to that value
    * returns false. Otherwise, returns true.
    *
    * This reducer does propogate [[NM]], in a sense, but the result is
    * `Value(false)` rather than `NM`. Unavailable values ([[NA]]) are
    * treated as the vaccuous case, so they will in count as a counter
    * example to the quantification.
    *
    * This reducer will only traverse the entire collection if it never
    * encounters a not meaningful value or a meaningful value that does
    * not satisfy the predicate `p`.
    *
    * @example {{{
    * Series.empty[Int, Int].reduce(ForAll[Int](i => false)) == Value(true)
    *
    * Series(1 -> 1, 2 -> 2).reduce(ForAll[Int](i => i < 3)) == Value(true)
    * Series(1 -> 1, 2 -> 2).reduce(ForAll[Int](i => i < 2)) == Value(false)
    *
    * Series.fromCells[Int, Int](1 -> NA)        .reduce(ForAll[Int](i => false)) == Value(true)
    * Series.fromCells[Int, Int](1 -> 1, 2 -> NM).reduce(ForAll[Int](i => i < 2)) == Value(false)
    * }}}
    *
    * @tparam  A  the value type of the column to reduce.
    * @param  p  the predicate to apply to the values of the column.
    * @return a [[Reducer]] that universally quantifies a predicate.
    * @see [[Exists]]
    */
  def ForAll[A](p: A => Boolean): Reducer[A, Boolean] = new ForAll(p)
}
