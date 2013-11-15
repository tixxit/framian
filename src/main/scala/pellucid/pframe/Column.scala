package pellucid
package pframe

import scala.collection.immutable.BitSet
import scala.reflect.runtime.universe.{ TypeTag, typeTag }

import shapeless._
import shapeless.syntax.typeable._

trait Column[A] {
  def typeTag: TypeTag[A]

  def exists(row: Int): Boolean
  def missing(row: Int): Missing
  def value(row: Int): A

  def apply(row: Int): Cell[A] =
    if (exists(row)) Value(value(row)) else missing(row)
}

object Column {
  def apply[A: TypeTag](values: Array[A]): Column[A] = DenseColumn(BitSet.empty, BitSet.empty, values)

  def apply[A: TypeTag](f: Int => A): Column[A] = InfiniteColumn(f)

  def apply[A: TypeTag](values: Map[Int, A]): Column[A] = MapColumn(values)

  def empty[A: TypeTag] = new EmptyColumn[A]

  def cast[A: Typeable: TypeTag](col: Column[_]): Column[A] = {
    if (col.typeTag.tpe <:< typeTag[A].tpe) {
      col.asInstanceOf[Column[A]]
    } else {
      new CastColumn[A](col)
    }
  }
}

final class EmptyColumn[A](implicit val typeTag: TypeTag[A]) extends Column[A] {
  def exists(row: Int): Boolean = false
  def missing(row: Int): Missing = NA
  def value(row: Int): A = throw new UnsupportedOperationException()
}

final class CastColumn[A: Typeable](col: Column[_])(implicit val typeTag: TypeTag[A]) extends Column[A] {
  def exists(row: Int): Boolean = col.exists(row) && col.apply(row).cast[A].isDefined
  def missing(row: Int): Missing = if (!col.exists(row)) col.missing(row) else NM
  def value(row: Int): A = col.value(row).cast[A].get
}

case class InfiniteColumn[A](f: Int => A)(implicit val typeTag: TypeTag[A]) extends Column[A] {
  def exists(row: Int): Boolean = true
  def missing(row: Int): Missing = NA
  def value(row: Int): A = f(row)
}

case class DenseColumn[A](naValues: BitSet, nmValues: BitSet, values: Array[A])(implicit val typeTag: TypeTag[A]) extends Column[A] {
  private final def valid(row: Int) = row >= 0 && row < values.length
  def exists(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def missing(row: Int): Missing = if (nmValues(row)) NM else NA
  def value(row: Int): A = values(row)
}

case class CellColumn[A](values: IndexedSeq[Cell[A]])(implicit val typeTag: TypeTag[A]) extends Column[A] {
  def exists(row: Int): Boolean = !values(row).isMissing
  def missing(row: Int): Missing = values(row) match {
    case Value(_) => throw new IllegalStateException()
    case (ms: Missing) => ms
  }
  def value(row: Int): A = values(row) match {
    case Value(x) => x
    case (_: Missing) => throw new IllegalStateException()
  }
}

case class MapColumn[A](values: Map[Int,A])(implicit val typeTag: TypeTag[A]) extends Column[A] {
  def exists(row: Int): Boolean = values contains row
  def missing(row: Int): Missing = NA
  def value(row: Int): A = values(row)
}

trait AsColumn[Col[_]] {
  def asColumn[V: TypeTag](col: Col[V]): Column[V]
}

object AsColumn {
  implicit def arrayColumn = new AsColumn[Array] {
    def asColumn[V: TypeTag](values: Array[V]): Column[V] = Column(values)
  }

  implicit def mapColumn = new AsColumn[({ type L[a] = Map[Int,a] })#L] {
    def asColumn[V: TypeTag](values: Map[Int,V]): Column[V] = Column(values)
  }
}
