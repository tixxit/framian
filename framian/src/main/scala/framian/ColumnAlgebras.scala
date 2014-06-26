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

import scala.{ specialized => spec }
import scala.annotation.{ unspecialized => unspec }

import spire.algebra._
import spire.syntax.all._

trait ColumnAlgebras0 {
  implicit def semiring[@spec(Int,Long,Float,Double) A: Semiring] = new ColumnSemiringImpl[A]
}

trait ColumnAlgebras1 extends ColumnAlgebras0 {
  implicit def rig[@spec(Int,Long,Float,Double) A: Rig] = new ColumnRigImpl[A]
}

trait ColumnAlgebras2 extends ColumnAlgebras1 {
  implicit def rng[@spec(Int,Long,Float,Double) A: Rng] = new ColumnRngImpl[A]
}

trait ColumnAlgebras3 extends ColumnAlgebras2 {
  implicit def ring[@spec(Int,Long,Float,Double) A: Ring] = new ColumnRingImpl[A]
}

trait ColumnAlgebras4 extends ColumnAlgebras3 {
  implicit def euclideanRing[@spec(Int,Long,Float,Double) A: EuclideanRing: Eq] = new ColumnEuclideanRingImpl[A]
}

trait ColumnAlgebras5 extends ColumnAlgebras4 {
  implicit def field[@spec(Int,Long,Float,Double) A: Field: Eq] = new ColumnFieldImpl[A]
}

trait ColumnAlgebras extends ColumnAlgebras5

@SerialVersionUID(0L)
final class ColumnSemiringImpl[@spec(Int,Long,Float,Double) A](implicit val algebra: Semiring[A]) extends ColumnSemiring[A]

@SerialVersionUID(0L)
final class ColumnRigImpl[@spec(Int,Long,Float,Double) A](implicit val algebra: Rig[A]) extends ColumnRig[A]

@SerialVersionUID(0L)
final class ColumnRngImpl[@spec(Int,Long,Float,Double) A](implicit val algebra: Rng[A]) extends ColumnRng[A]

@SerialVersionUID(0L)
final class ColumnRingImpl[@spec(Int,Long,Float,Double) A](implicit val algebra: Ring[A]) extends ColumnRing[A]

@SerialVersionUID(0L)
final class ColumnEuclideanRingImpl[@spec(Int,Long,Float,Double) A](implicit val algebra: EuclideanRing[A], val order: Eq[A]) extends ColumnEuclideanRing[A]

@SerialVersionUID(0L)
final class ColumnFieldImpl[@spec(Int,Long,Float,Double) A](implicit val algebra: Field[A], val order: Eq[A]) extends ColumnField[A]

private trait UnOpColumn[@spec(Int,Long,Float,Double) A] extends Column[A] {
  def arg: Column[A]

  def isValueAt(row: Int): Boolean = arg.isValueAt(row)
  def nonValueAt(row: Int): NonValue = arg.nonValueAt(row)
}

private trait BinOpColumn[@spec(Int,Long,Float,Double) A] extends Column[A] {
  def lhs: Column[A]
  def rhs: Column[A]

  def isValueAt(row: Int): Boolean = lhs.isValueAt(row) && rhs.isValueAt(row)
  def nonValueAt(row: Int): NonValue = if (lhs.isValueAt(row)) {
    rhs.nonValueAt(row)
  } else if (rhs.isValueAt(row)) {
    lhs.nonValueAt(row)
  } else {
    (lhs.nonValueAt(row), rhs.nonValueAt(row)) match {
      case (NA, _) => NA
      case (_, NA) => NA
      case (NM, NM) => NM
    }
  }
}

trait ColumnSemiring[@spec(Int,Long,Float,Double) A] extends Semiring[Column[A]] {
  implicit def algebra: Semiring[A]

  def zero: Column[A] = Column.const(algebra.zero)

  def plus(x: Column[A], y: Column[A]): Column[A] = new BinOpColumn[A] {
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = lhs.valueAt(row) + rhs.valueAt(row)
  }

  def times(x: Column[A], y: Column[A]): Column[A] = new BinOpColumn[A] {
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = lhs.valueAt(row) * rhs.valueAt(row)
  }
}

trait ColumnRig[@spec(Int,Long,Float,Double) A] extends ColumnSemiring[A] with Rig[Column[A]] {
  implicit def algebra: Rig[A]

  def one: Column[A] = Column.const(algebra.one)
}

trait ColumnRng[@spec(Int,Long,Float,Double) A] extends ColumnSemiring[A] with Rng[Column[A]] {
  implicit def algebra: Rng[A]

  def negate(x: Column[A]): Column[A] = new UnOpColumn[A] {
    val arg = x
    def valueAt(row: Int): A = -arg.valueAt(row)
  }

  override def minus(x: Column[A], y: Column[A]): Column[A] = new BinOpColumn[A] {
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = lhs.valueAt(row) - rhs.valueAt(row)
  }
}

trait ColumnRing[@spec(Int,Long,Float,Double) A]
    extends ColumnRig[A] with ColumnRng[A] with Ring[Column[A]] {
  override def algebra: Ring[A]
  override def zero: Column[A] = Column.const(algebra.zero)
  override def fromInt(n: Int): Column[A] = Column.const(algebra.fromInt(n))
}

private trait DivOpColumn[@spec(Int,Long,Float,Double) A] extends Column[A] {
  implicit def algebra: AdditiveMonoid[A]
  implicit def order: Eq[A]
  def lhs: Column[A]
  def rhs: Column[A]

  def isValueAt(row: Int): Boolean =
    lhs.isValueAt(row) && rhs.isValueAt(row) && (rhs.valueAt(row) =!= algebra.zero)

  def nonValueAt(row: Int): NonValue = {
    val lhsIsValue = lhs.isValueAt(row)
    val rhsIsValue = rhs.isValueAt(row)
    if (lhsIsValue && rhsIsValue) NM
    else if (lhsIsValue) rhs.nonValueAt(row)
    else if (rhsIsValue) lhs.nonValueAt(row)
    else (lhs.nonValueAt(row), rhs.nonValueAt(row)) match {
      case (NA, _) => NA
      case (_, NA) => NA
      case _ => NM
    }
  }
}

trait ColumnEuclideanRing[@spec(Int,Long,Float,Double) A]
    extends ColumnRing[A] with EuclideanRing[Column[A]] { self =>
  override implicit def algebra: EuclideanRing[A]
  implicit def order: Eq[A]

  def quot(x: Column[A], y: Column[A]): Column[A] = new DivOpColumn[A] {
    implicit val algebra = self.algebra
    val order = self.order
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = lhs.valueAt(row) /~ rhs.valueAt(row)
  }

  def mod(x: Column[A], y: Column[A]): Column[A] = new DivOpColumn[A] {
    implicit val algebra = self.algebra
    val order = self.order
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = lhs.valueAt(row) % rhs.valueAt(row)
  }

  def gcd(x: Column[A], y: Column[A]): Column[A] = new BinOpColumn[A] {
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = algebra.gcd(lhs.valueAt(row), rhs.valueAt(row))
  }
}

trait ColumnField[@spec(Int,Long,Float,Double) A]
    extends ColumnEuclideanRing[A] with Field[Column[A]] { self =>
  override implicit def algebra: Field[A]

  def div(x: Column[A], y: Column[A]): Column[A] = new DivOpColumn[A] {
    implicit val algebra = self.algebra
    val order = self.order
    val lhs = x
    val rhs = y
    def valueAt(row: Int): A = lhs.valueAt(row) / rhs.valueAt(row)
  }

  override def fromDouble(x: Double) = Column.const(algebra.fromDouble(x))
}
