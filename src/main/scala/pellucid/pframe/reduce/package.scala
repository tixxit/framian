package pellucid.pframe

import scala.annotation.tailrec

import spire.algebra._

package object reduce {
  def MonoidReducer[A: Monoid]: Reducer[A, A] = new MonoidReducer[A]

  def Mean[A: Field]: Reducer[A, A] = new Mean[A]

  def Sum[A: AdditiveMonoid]: Reducer[A, A] = MonoidReducer(spire.algebra.Monoid.additive[A])

  def Max[A: Order]: Reducer[A, Option[A]] = new Max[A]

  def Min[A: Order]: Reducer[A, Option[A]] = Max(Order[A].reverse)
}

