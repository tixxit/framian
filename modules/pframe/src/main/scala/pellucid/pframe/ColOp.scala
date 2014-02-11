package pellucid.pframe

import scala.annotation.implicitNotFound
import scala.collection.SortedMap
import scala.reflect.ClassTag

import spire.algebra._
import spire.implicits._

import shapeless._
import shapeless.ops.function._
import shapeless.ops.nat._
import Nat.{ _1, _2, _3 }

trait ColOp[Row, Col, A] {
  type Result

  def apply(frame: Frame[Row, Col], cols: List[Col], extractor: RowExtractor[A, Col, _]): Result
}

final class ColSelector[Row, Col, Op[R, C, A] <: ColOp[R, C, A]](frame: Frame[Row, Col]) {
  def apply[A: ColumnTyper](col: Col)(implicit op: Op[Row, Col, A]): op.Result =
    op.apply(frame, col :: Nil, RowExtractor[A, Col, Fixed[_1]])

  def apply[A: ColumnTyper, B: ColumnTyper](c0: Col, c1: Col)(implicit op: Op[Row, Col, (A, B)]): op.Result =
    op.apply(frame, c0 :: c1 :: Nil, RowExtractor[(A, B), Col, Fixed[_2]])

  def apply[A: ColumnTyper, B: ColumnTyper, C: ColumnTyper](c0: Col, c1: Col, c2: Col)(implicit op: Op[Row, Col, (A, B, C)]): op.Result =
    op.apply(frame, c0 :: c1 :: c2 :: Nil, RowExtractor[(A, B, C), Col, Fixed[_3]])
}





final class TypeWitness[A](val value: A)(implicit val classTag: ClassTag[A])
object TypeWitness {
  implicit def lift[A: ClassTag](a: A) = new TypeWitness[A](a)
}

// TODO: Rename to Rec or Record
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

  def apply[K](cols: Series[K, UntypedColumn], row: Int): Rec[K] =
    new Rec(cols, row)

  implicit def RecRowExtractor[K]: RowExtractor[Rec[K], K, Variable] = new RowExtractor[Rec[K], K, Variable] {
    type P = Series[K, UntypedColumn]

    def prepare[R](frame: Frame[R, K], cols: List[K]): Option[P] = {
      val s = frame.columnsAsSeries
      import s.index.{ order, classTag }
      Some(Series.fromCells(cols map { k => k -> s(k) }: _*))
    }

    def extract[R](frame: Frame[R, K], key: R, row: Int, cols: P): Cell[Rec[K]] =
      Value(Rec(cols, row))
  }
}

sealed trait Cols[K, A] {
  val extractor: RowExtractor[A, K, _]
  def getOrElse(all: => List[K]) = fold(all)(keys => keys)
  def fold[B](all: => B)(f: List[K] => B): B
  def map[B](f: A => B): Cols[K, B]
}

object Cols extends ColsFunctions {
  sealed trait SizedCols[K, Sz <: Size, A] extends Cols[K, A] {
    val extractor: RowExtractor[A, K, Sz]

    def fold[B](all: => B)(f: List[K] => B): B
    def as[B](implicit extractor0: RowExtractor[B, K, Sz]): SizedCols[K, Sz, B]
  }

  final class AllCols[K, A](val extractor: RowExtractor[A, K, Variable])
      extends SizedCols[K, Variable, A] {
    def fold[B](all: => B)(f: List[K] => B): B = all
    def as[B](implicit extractor0: RowExtractor[B, K, Variable]): AllCols[K, B] =
      new AllCols[K, B](extractor0)
    def map[B](f: A => B): AllCols[K, B] =
      new AllCols[K, B](extractor map f)
  }

  final class SomeCols[K, S <: Size, A](val keys: List[K], val extractor: RowExtractor[A, K, S])
      extends SizedCols[K, S, A] {
    def fold[B](all: => B)(f: List[K] => B): B = f(keys)
    def as[B](implicit extractor0: RowExtractor[B, K, S]): SomeCols[K, S, B] =
      new SomeCols[K, S, B](keys, extractor0)
    def map[B](f: A => B): SomeCols[K, S, B] =
      new SomeCols[K, S, B](keys, extractor map f)
  }
}

trait ColsFunctions {
  import Cols._
  import Nat._

  def all[K] = new AllCols[K, Rec[K]](RowExtractor[Rec[K], K, Variable])

  def sized[K, N <: Nat](s: Sized[List[K], N]): SomeCols[K, Fixed[N], Rec[K]] =
    new SomeCols[K, Fixed[N], Rec[K]](s.unsized, RowExtractor[Rec[K], K, Fixed[N]])

  def apply[K](c0: K): SomeCols[K, Fixed[_1], Rec[K]] =
    sized(Sized[List](c0))

  def apply[K](c0: K, c1: K): SomeCols[K, Fixed[_2], Rec[K]] =
    sized(Sized[List](c0, c1))
}

// cols(0, 1).as[(Double, Double)]
// 
// f.group(Col(0).as[Double], 1.as[Double])
// 
// f.group[Double, Double](0, 1).as[(Double, Double)]
// f.group[Double, Double](0, 1).by(f)
// 
// f.map[Double, Double](0, 1).to(2)(_ + _)
// f.mapTo(2).cols[Double, Double](0, 1)(_ + _)
// 
// f.group[LocalDate](date)
// f.groupBy[LocalDate](date) { _.toString }
// f.group[Json](0, 1, 2)
// f.cols[Double, Double](0, 1).mapTo(2)(_ + _)
// f.group.rowAs[List[String]]
// f.map.rowAs[Json]
// 
// f.mapTo(2)
// f.keepColumns(0, 1).as[JValue]

// final class ColSelectorWithMap[Row, Col, Op[R, C, A] <: ColOp[R, C, A]](frame: Frame[Row, Col]) {
//   def apply[A, B: ColumnTyper](col: Col)(f: A => B)(implicit op: Op[Row, Col, A]): op.Result =
//     op.apply(frame, col :: Nil, RowExtractor[A, Col, Fixed[_1]])
// }

@implicitNotFound(msg = "Cannot find Order[${A}] and/or ClassTag[${A}] required for grouping")
final class GroupOp[Row, Col, A: ClassTag: Order] extends ColOp[Row, Col, A] {
  type Result = Frame[A, Col]

  def apply(frame: Frame[Row, Col], cols: List[Col], extractor: RowExtractor[A, Col, _]): Result =
    GroupOps.groupBy0[Row, Col, A, A](frame, cols, a => a, _ => None, extractor)
}

object GroupOp {
  implicit def create[Row, Col, A: ClassTag: Order] = new GroupOp[Row, Col, A]
}

final class GroupByOp[Row, Col, A] extends ColOp[Row, Col, A] {
  final class Result(frame: Frame[Row, Col], cols: List[Col], extractor: RowExtractor[A, Col, _]) {
    def apply[F, B: Order: ClassTag](f: F)(implicit fntop: FnToProduct.Aux[F, A => B]): Frame[B, Col] =
      GroupOps.groupBy0[Row, Col, A, B](frame, cols, fntop(f), _ => None, extractor)
  }

  def apply(frame: Frame[Row, Col], cols: List[Col], extractor: RowExtractor[A, Col, _]): Result =
    new Result(frame, cols, extractor)
}

object GroupByOp {
  implicit def create[Row, Col, A] = new GroupByOp[Row, Col, A]
}

object GroupOps {
  def groupBy0[Row, Col, A, B: Order: ClassTag](frame: Frame[Row, Col], cols: List[Col],
      f: A => B, g: Missing => Option[B], extractor: RowExtractor[A, Col, _]): Frame[B, Col] = {
    import spire.compat._

    var groups: SortedMap[B, List[Int]] = SortedMap.empty // TODO: Lots of room for optimization here.
    for (p <- extractor.prepare(frame, cols)) {
      frame.rowIndex foreach { (key, row) =>
        extractor.extract(frame, key, row, p) match {
          case Value(group0) =>
            val group = f(group0)
            groups += (group -> (row :: groups.getOrElse(group, Nil)))
          case (missing: Missing) =>
            g(missing) foreach { group =>
              groups += (group -> (row :: groups.getOrElse(group, Nil)))
            }
        }
      }
    }

    val (keys, rows) = (for {
      (group, rows) <- groups.toList
      row <- rows.reverse
    } yield (group -> row)).unzip
    val groupedIndex = Index.ordered(keys.toArray, rows.toArray)
    frame.withRowIndex(groupedIndex)
  }
}
