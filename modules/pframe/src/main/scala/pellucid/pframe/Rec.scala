package pellucid.pframe

import scala.reflect.ClassTag

import spire.algebra.Order

final class TypeWitness[A](val value: A)(implicit val classTag: ClassTag[A])
object TypeWitness {
  implicit def lift[A: ClassTag](a: A) = new TypeWitness[A](a)
}

/**
 * A `Rec` is an untyped sequence of values - usually corresponding to a row or
 * column in a Frame.
 */
final class Rec[K](cols: Series[K, UntypedColumn], row: Int) {
  def get[A: ColumnTyper](col: K): Cell[A] =
    cols(col) flatMap (_.cast[A].apply(row))

  def values: Iterable[(K, Cell[Any])] = cols map { case (k, colCell) =>
    val value = for {
      col <- colCell
      a <- col.cast[Any].apply(row)
    } yield a

    k -> value
  }

  override def toString: String = values.map { case (k, value) =>
    s"""$k -> ${value.fold("na", "nm")(_.toString)}"""
  }.mkString("Rec(", ", ", ")")

  override def equals(that: Any): Boolean = that match {
    case (that: Rec[_]) => this.values == that.values
    case _ => false
  }

  override def hashCode: Int = this.values.hashCode * 23
}

object Rec {
  def apply[K: Order: ClassTag](kvs: (K, TypeWitness[_])*): Rec[K] = {
    val cols: Series[K, UntypedColumn] = Series(kvs.map { case (k, w: TypeWitness[a]) =>
      k -> TypedColumn[a](Column.const(w.value))(w.classTag)
    }: _*)
    new Rec(cols, 0)
  }

  def fromRow[K](frame: Frame[_, K])(row: Int): Rec[K] =
    new Rec(frame.columnsAsSeries, row)

  def fromCol[K](frame: Frame[K, _])(col: Int): Rec[K] =
    new Rec(frame.rowsAsSeries, col)

  implicit def RecRowExtractor[K]: RowExtractor[Rec[K], K, Variable] = new RowExtractor[Rec[K], K, Variable] {
    type P = Series[K, UntypedColumn]

    def prepare[R](frame: Frame[R, K], cols: List[K]): Option[P] = {
      val s = frame.columnsAsSeries
      import s.index.{ order, classTag }
      Some(Series.fromCells(cols map { k => k -> s(k) }: _*))
    }

    def extract[R](frame: Frame[R, K], key: R, row: Int, cols: P): Cell[Rec[K]] =
      Value(new Rec(cols, row))
  }
}
