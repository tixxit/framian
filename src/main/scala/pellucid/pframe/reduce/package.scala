package pellucid.pframe

import scala.annotation.tailrec
import scala.reflect.ClassTag

import spire.algebra._

package object reduce {
  def MonoidReducer[A: Monoid]: Reducer[A, A] = new MonoidReducer[A]

  def Mean[A: Field]: Reducer[A, Option[A]] = new Mean[A]

  def Sum[A: AdditiveMonoid]: Reducer[A, A] = MonoidReducer(spire.algebra.Monoid.additive[A])

  def Median[A: Field: Order: ClassTag]: Reducer[A, Option[A]] = new Median

  def Max[A: Order]: Reducer[A, Option[A]] = new Max[A]

  def Min[A: Order]: Reducer[A, Option[A]] = Max(Order[A].reverse)
}

