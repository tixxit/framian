package pellucid.pframe

import scala.reflect.{ ClassTag, classTag }
import scala.{ specialized => spec }

import shapeless._

trait ColumnTyper[@spec(Int,Long,Float,Double) A] {
  def cast(col: TypedColumn[_]): Column[A]
}

object ColumnTyper extends ColumnTyperInstances

trait ColumnTyper0 {
  implicit def defaultTyper[A: ClassTag] = new DefaultColumnTyper[A]
}

trait ColumnTyper1 extends ColumnTyper0 {
  implicit def typeableTyper[A: ClassTag: Typeable] = new TypeableColumnTyper[A]
}

trait ColumnTyper2 extends ColumnTyper1 {
  implicit val anyTyper = new ColumnTyper[Any] {
    def cast(col: TypedColumn[_]): Column[Any] = col.column.asInstanceOf[Column[Any]]
  }
}

trait ColumnTyper3 extends ColumnTyper2 {
  implicit val int = new IntColumnTyper
  implicit val long = new LongColumnTyper
  implicit val float = new FloatColumnTyper
  implicit val double = new DoubleColumnTyper
  implicit val bigInt = new BigIntTyper
  implicit val bigDecimal = new BigDecimalTyper
  implicit val rational = new RationalTyper
  implicit val number = new NumberTyper
}

trait ColumnTyperInstances extends ColumnTyper3

final class DefaultColumnTyper[A: ClassTag] extends ColumnTyper[A] {
  def cast(col: TypedColumn[_]): Column[A] =
    if (classTag[A].runtimeClass isAssignableFrom col.classTagA.runtimeClass) {
      col.column.asInstanceOf[Column[A]]
    } else {
      Column.empty[A]
    }
}

final class TypeableColumnTyper[A: ClassTag: Typeable] extends ColumnTyper[A] {
  def cast(col: TypedColumn[_]): Column[A] =
    if (classTag[A].runtimeClass isAssignableFrom col.classTagA.runtimeClass) {
      col.column.asInstanceOf[Column[A]]
    } else {
      new CastColumn[A](col.column)
    }
}
