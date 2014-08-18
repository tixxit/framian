/*  _____                    _
 * |  ___| __ __ _ _ __ ___ (_) __ _ _ __
 * | |_ | '__/ _` | '_ ` _ \| |/ _` | '_ \
 * |  _|| | | (_| | | | | | | | (_| | | | |
 * |_|  |_|  \__,_|_| |_| |_|_|\__,_|_| |_|
 *
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package framian

import scala.language.implicitConversions

import spire.algebra.{ Eq, Order, Semigroup, Monoid }
import spire.syntax.order._
import spire.syntax.semigroup._

/** A [[Cell]] represents a single piece of data that may not be
  * available or meangingful in a given context.
  *
  * Essentially, a [[Cell]] is similar to `Option`, except instead of
  * `None` we have 2 representations of [[NonValue]], the absence of
  * data: [[NA]] (Not Available) and [[NM]] (Not Meaningful).
  *
  * @tparam A the value type contain in the cell
  * @see [[Value]] [[[NonValue]]] [[NA]] [[NM]]
  */
sealed trait Cell[+A] {

  /** Returns true if this [[Cell]] is a value that is available and meaningful.
    *
    * @return true if this [[Cell]] is a value that is available and meaningful.
    * @see [[Value]]
    */
  def isValue: Boolean

  /** Returns true if this [[Cell]] is not a value that is available and meaningful.
    *
    * @return true if this [[Cell]] is not a value that is available and meaningful.
    * @see [[NonValue]]
    * @see [[NA]]
    * @see [[NM]]
    */
  @inline final def isNonValue: Boolean = !isValue

  /** Return the [[Cell]]'s value.
    *
    * @note The [[Cell]]'s value must be both available and meaningful.
    * @throws NoSuchElementException if the value is unavailable or not meaningful.
    */
  def get: A

  /** Project this [[Cell]] to an `Option`
    *
    * @return Some of the value or None for non values
    */
  def value: Option[A]

  /** Returns the contents of the [[Cell]] as a `String`
    *
    * @note {{{
    * NA.valueString == "NA"
    * NA.valueString == "NM"
    * Value(1D).valueString == "1.0"
    *
    * NA.toString == "NA"
    * NM.toString == "NM"
    * Value(1D).toString == "Value(1.0)"
    * }}}
    *
    * @return the content of the [[Cell]] as a `String`.
    */
  def valueString: String

  /** Returns the result of applying `f` to this [[Cell]]'s value.
    * Otherwise, evaluates expression `na` if a value is not
    * available, or evaluates expression `nm` if the value is
    * not meaningful.
    *
    * @param  na  the expression to evaluate if no value is available.
    * @param  nm  the expression to evaluate if the value in not meaningful.
    * @param  f   the function to apply to a value that is available and meaningful.
    */
  @inline final def fold[B](na: => B, nm: => B)(f: A => B): B = this match {
    case NA => na
    case NM => nm
    case Value(a) => f(a)
  }

  /** Returns the [[Cell]]'s value if it is available and meaningful,
    * otherwise returns the result of evaluating expression `default`.
    *
    * @param  default  the default expression.
    * @see [[get]]
    * @see [[orElse]]
    */
  @inline final def getOrElse[B >: A](default: => B): B =
    if (isValue) this.get else default

  /** Returns a [[Value]] containing the result of applying `f` to
    * this [[Cell]]'s value if it is available and meaningful.
    *
    * @param  f  the function to apply to a value that is available and meaningful.
    * @see [[flatMap]]
    * @see [[foreach]]
    */
  @inline final def map[B](f: A => B): Cell[B] =
    if (isValue) Value(f(this.get)) else this.asInstanceOf[Cell[B]]

  /** Returns the result of applying `f` to this [[Cell]]'s value if
    * it is available and meaningful.
    *
    * @param  f  the function to apply to a value that is available and meaningful.
    * @see [[map]]
    * @see [[foreach]]
    */
  @inline final def flatMap[B](f: A => Cell[B]): Cell[B] =
    if (isValue) f(this.get) else this.asInstanceOf[Cell[B]]

  /** Flatten a nested [[Cell]] with type `Cell[Cell[B]]` into `Cell[B]`.
    *
    * @note there must be implicit evident that `A` is a subtype of `Cell[B]`.
    */
  @inline final def flatten[B](implicit ev: A <:< Cell[B]): Cell[B] =
    if (isValue) ev(this.get) else this.asInstanceOf[Cell[B]]

  /** Returns this [[Cell]] unless it contains a value that is
    * available and meaningful ''and'' applying the predicate `p` to
    * that value returns false, then return [[NA]].
    *
    * @param  p  the predicate used to test a value that is available and meaningful.
    * @see [[filterNot]]
    * @see [[collect]]
    */
  @inline final def filter(p: A => Boolean): Cell[A] =
    if (isNonValue || p(this.get)) this else NA

  /** Returns this [[Cell]] unless it contains a value that is
    * available and meaningful ''and'' applying the predicate `p` to
    * that value returns true, then return [[NA]].
    *
    * @param  p  the predicate to test a value that is available and meaningful.
    * @see [[filter]]
    * @see [[collect]]
    */
  @inline final def filterNot(p: A => Boolean): Cell[A] =
    if (isNonValue || !p(this.get)) this else NA

  /** Returns true if this [[Cell]]'s value is available and
    * meaningful ''and'' the predicate `p` returns true when applied
    * to that value. Otherwise, returns false.
    *
    * @param  p  the predicate to test a value that is available and meaningful.
    * @see [[forall]]
    */
  @inline final def exists(p: A => Boolean): Boolean =
    isValue && p(this.get)

  /** Returns true if this [[Cell]]'s value is unavailable ([[NA]])
    *''or'' the predicate `p` returns true when applied to this
    * [[Cell]]'s meaningful value.
    *
    * @note [[NA]] represents the vacuous case, so will result in
    * true, but [[NM]] will result in false.
    *
    * @param  p  the predicate to test a value that is available and meaningful.
    * @see [[exists]]
    */
  @inline final def forall(p: A => Boolean): Boolean = this match {
    case NA => true
    case NM => false
    case Value(a) => p(a)
  }

  /** Apply the the given procedure `f` to the [[Cell]]'s value if it
    * is available and meaningful. Otherwise, do nothing.
    *
    * @param  f  the procedure to apply to a value that is available and meaningful.
    * @see [[map]]
    * @see [[flatMap]]
    */
  @inline final def foreach[U](f: A => U): Unit =
    if (isValue) f(this.get)

  /** Returns a [[Value]] containing the result of appling `pf` to
    * this [[Cell]]'s value if it is available and meaningful ''and''
    * `pf` is defined for that value. Otherwise return [[NA]], unless
    * this [[Cell]] is [[NM]].
    *
    * @param  pf  the partial function to apply to a value that is available and meaningful.
    * @see [[filter]]
    * @see [[filterNot]]
    */
  @inline final def collect[B](pf: PartialFunction[A, B]): Cell[B] = this match {
    case NM => NM
    case Value(a) if pf.isDefinedAt(a) => Value(pf(a))
    case _ => NA
  }

  /** Returns this [[Cell]] if its value is available and meaningful,
    * otherwise return the result of evaluating `alternative`.
    *
    * @param  alternative  the alternative expression
    * @see [[getOrElse]]
    */
  @inline final def orElse[B >: A](alternative: => Cell[B]): Cell[B] =
    if (isValue) this else alternative

  /** Project this [[Cell]] to an `Option`
    *
    * @return Some of the value or None for non values
    */
  @inline final def toOption: Option[A] = value

  /** Returns a singleton list containing the [[Cell]]'s value, or
    * the empty list if the [[Cell]]'s value is unavailable or not
    * meaningful.
    */
  @inline final def toList: List[A] =
    if (isValue) List(this.get) else List.empty

  /** If both `this` and `that` are values, then this returns a value derived
    * by applying `f` to the values of them. Otherwise, if either `this` or
    * `that` is `NM`, then `NM` is returned, otherwise `NA` is returned.
    */
  @inline def zipMap[B, C](that: Cell[B])(f: (A, B) => C): Cell[C] = (this, that) match {
    case (Value(a), Value(b)) => Value(f(a, b))
    case (NM, _) | (_, NM) => NM
    case _ => NA
  }
}

// TODO: there are currently issues where we get comparison between Value(NA) and NA and this should be true
// the current tweaks to equality are just holdovers until we figure out some more details on the implementation
// of non values.
object Cell extends CellInstances {
  def value[A](x: A): Cell[A] = Value(x)
  def notAvailable[A]: Cell[A] = NA
  def notMeaningful[A]: Cell[A] = NM

  def fromOption[A](opt: Option[A], nonValue: NonValue = NA): Cell[A] = opt match {
    case Some(a) => Value(a)
    case None => nonValue
  }
}

/** The supertype of non values, [[NA]] (''Not Available'') and
  * [[NM]] (''Not Meaningful'')
  *
  * @see [[Cell]] [[NA]] [[NM]]
  */
sealed trait NonValue extends Cell[Nothing] {
  def isValue = false
  def value = None

  override def equals(that: Any): Boolean = that match {
    case Value(thatValue) => this == thatValue
    case _ => super.equals(that)
  }
}


/** A value is ''Not Available (NA)''
  *
  * This represents the absence of any data.
  *
  * @see [[Cell]] [[NonValue]] [[NM]] [[Value]]
  */
case object NA extends NonValue {
  def get = throw new NoSuchElementException("NA.get")
  val valueString = "NA"
}


/** The value is ''Not Meaningful (NM)''.
  *
  * This indicates that data exists, but that it is not meaningful.
  * For instance, if we divide by 0, then the result is not
  * meaningful, but we wouldn't necessarily say that data is
  * unavailable.
  *
  * @see [[Cell]] [[NonValue]] [[NA]] [[Value]]
  */
case object NM extends NonValue {
  def get = throw new NoSuchElementException("NM.get")
  val valueString = "NM"
}


/** A value that is meaningful.
  *
  * @tparam A the type of the value contained
  * @see [[Cell]] [[NonValue]] [[NA]] [[NM]]
  */
final case class Value[+A](get: A) extends Cell[A] {
  def value = Some(get)
  def valueString = get.toString

  val isValue = if (get == NA || get == NM) false else true

  override def equals(that: Any): Boolean = that match {
    case Value(Value(NA)) => get == NA
    case Value(Value(NM)) => get == NM
    case Value(thatValue) => thatValue == get
    case v @ NA => get == NA
    case v @ NM => get == NM
    case _ => false
  }
}

object CellInstances {
  trait CellInstances0 {
    implicit def cellEq[A: Eq]: Eq[Cell[A]] = new CellEq[A]
  }
}


trait CellInstances extends CellInstances.CellInstances0 {
  implicit def cellOrder[A: Order]: Order[Cell[A]] = new CellOrder[A]
  implicit def cellMonoid[A: Semigroup]: Monoid[Cell[A]] = new CellMonoid[A]
}

@SerialVersionUID(0L)
private final class CellEq[A: Eq] extends Eq[Cell[A]] {
  import spire.syntax.eq._

  def eqv(x: Cell[A], y: Cell[A]): Boolean = (x, y) match {
    case (Value(x0), Value(y0)) => x0 === y0
    case (NA, NA) | (NM, NM) => true
    case _ => false
  }

  /*def eqv[X >: A: Eq, Y >: A: Eq](x: Cell[X], y: Cell[Y]): Boolean = (x, y) match {
    case (Value(NA), NA) | (Value(NM), NM) | (NA, Value(NA)) | (NM, Value(NM)) => true
    case _ => false
  }*/
}

@SerialVersionUID(0L)
private final class CellOrder[A: Order] extends Order[Cell[A]] {
  def compare(x: Cell[A], y: Cell[A]): Int = (x, y) match {
    case (Value(x0), Value(y0)) => x0 compare y0
    case (NA, NA) | (NM, NM) => 0
    case (NA, _) => -1
    case (_, NA) => 1
    case (NM, _) => -1
    case (_, NM) => 1
  }
}

@SerialVersionUID(0L)
private final class CellMonoid[A: Semigroup] extends Monoid[Cell[A]] {
  def id: Cell[A] = NA
  def op(x: Cell[A], y: Cell[A]): Cell[A] = (x, y) match {
    case (NM, _) => NM
    case (_, NM) => NM
    case (Value(a), Value(b)) => Value(a |+| b)
    case (Value(_), _) => x
    case (_, Value(_)) => y
    case (NA, NA) => NA
  }
}
