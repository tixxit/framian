package framian

import spire.algebra.Order

import scala.reflect.ClassTag

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

trait CellGenerators {

  val dDefault = (60, 3, 1)

  /** Generate a cell that can produce one of [[Value]], [[NA]] or [[NM]] cells based on a given
    * weighted probability distribution.
    *
    * @param gen    The generator that generates the inner values for the [[Value]] cell
    * @param weight The weighted probability that either a [[Value]] (`_1`), [[NA]] (`_2`) or [[NM]]
    *               (`_3`) will be generated. E.g., (1, 1, 1), leaves a 33% chance for each
    * @return       A generator that will produce the provided weighted distribution of cell types
    */
  def genCell[A](gen: Gen[A], weight: (Int, Int, Int) = dDefault): Gen[Cell[A]] = {
    gen.flatMap { genVal =>
      Gen.frequency(
        (weight._1, gen.map(Value(_))),
        (weight._2, Gen.const(NA)),
        (weight._3, Gen.const(NM))
      )
    }
  }
}
object CellGenerators extends CellGenerators

trait ColumnGenerators {

  /** A generator for a higher-order arbitrary column that generates a column who has arbitrary
    * values
    */
  implicit def arbColumn[A: Arbitrary: ClassTag]: Arbitrary[Column[A]] =
        Arbitrary(genColumn(arbitrary[A]))

  /** Generate a [[framian.Column]] with the provided distribution of cell types.
    *
    * @param gen    The generator for the cell values
    * @param weight The distribution of cell types in the column between [[Value]], [[NA]] and
    *               [[NM]], respectively
    * @return       A generator for columns
    */
  def genColumn[A: ClassTag](gen: Gen[A], weight: (Int, Int, Int) = CellGenerators.dDefault): Gen[Column[A]] = for {
    cellValues <- Gen.listOf(CellGenerators.genCell(gen, weight))
  } yield Column(cellValues: _*)
}
object ColumnGenerators extends ColumnGenerators

trait SeriesGenerators {

  /** Wrapper class to specialize "empty" series for use with arbitrary generators
    *
    * @param series   The series instance to wrap
    */
  case class EmptySeries[K, V](series: Series[K, V])

  /** Wrapper class to specialize "meaningful" series for use with arbitrary generators
    *
    * @param series   The series instance to wrap
    */
  case class MeaningfulSeries[K, V](series: Series[K, V])

  /**
   * @return an arbitrary series generator that generates series that contain all potential types of
   *         cell types ([[Value]], [[NA]], [[NM]])
   */
  implicit def arbSeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag]: Arbitrary[Series[K, V]] =
    Arbitrary(SeriesGenerators.genSeries(arbitrary[K], arbitrary[V], (60, 30, 1)))

  /**
   * @return an arbitrary series generator that generates series that contain only [[NA]] cells
   */
  implicit def arbEmptySeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag]: Arbitrary[EmptySeries[K, V]] =
    Arbitrary(SeriesGenerators.genSeries(arbitrary[K], arbitrary[V], (0, 1, 0)).map(EmptySeries(_)))

  /**
   * @return an arbitrary series generator that generates series that contain only [[Value]] and
   *         [[NA]] cells
   */
  implicit def arbMeaningfulSeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag]: Arbitrary[MeaningfulSeries[K, V]] =
    Arbitrary(SeriesGenerators.genSeries(arbitrary[K], arbitrary[V], (7, 3, 0)).map(MeaningfulSeries(_)))

  /** Generate a [[framian.Series]] whose keys and values come from the given generators. The cell
    * types will be split among [[Value]], [[NA]] and [[NM]] according to the provided weighted
    * distribution.
    *
    * @param keyGen The generator for series keys
    * @param valGen The generator for series values
    * @param weight The weighted distribution of cell types [[Value]], [[NA]], [[NM]], respectively
    * @return       A generator for series
    */
  def genSeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V], weight: (Int, Int, Int) = CellGenerators.dDefault) =
    for {
      t2s <- Gen.listOf(Gen.zip(keyGen, CellGenerators.genCell(valGen, weight)))
    } yield Series.fromCells(t2s: _*)
}
object SeriesGenerators extends SeriesGenerators

trait IndexGenerators {
  def genIndex[K: Order: ClassTag](keyGen: Gen[K]): Gen[Index[K]] =
    Gen.listOf(Gen.zip(keyGen, arbitrary[Int])).map(pairs => Index(pairs: _*))

  implicit def arbIndex[K: Arbitrary: Order: ClassTag]: Arbitrary[Index[K]] =
    Arbitrary(genIndex(arbitrary[K]))
}
object IndexGenerators extends IndexGenerators

trait FrameGenerators {
  import shapeless.{Generic, HList, HNil, Poly2}
  import shapeless.ops.hlist.LeftFolder
  import shapeless.syntax._

  def genFrameWithCol[R: Order: ClassTag, C, A: ClassTag](genFrame: Gen[Frame[R, C]], col: C, genCell: Gen[Cell[A]]): Gen[Frame[R, C]] = {
    genFrame.flatMap { frame =>
      val seriesGen: Gen[Series[R, A]] = Gen.sequence[Vector[(R, Cell[A])], (R, Cell[A])](frame.rowIndex.map { case (key, _) =>
        genCell.map(key -> _)
      }).map(Series.fromCells(_))

      seriesGen.map { series =>
        frame.merge(col, series)(Merge.Outer)
      }
    }
  }

  object coFrameGen extends Poly2 {
    implicit def valuesCase[Row: Order: ClassTag, Col, A: ClassTag] =
      at[Gen[Frame[Row, Col]], (Col, Gen[A])] { case (genFrame, (col, genValue)) =>
        genFrameWithCol(genFrame, col, genValue.map(Value(_)))
      }

    implicit def cellCase[Row: Order: ClassTag, Col, A: ClassTag] =
      at[Gen[Frame[Row, Col]], (Col, Gen[Cell[A]])] { case (genFrame, (col, genCell)) =>
        genFrameWithCol(genFrame, col, genCell)
      }
  }

  type COFrameGenFolder[L <: HList, Row, Col] = LeftFolder.Aux[L, Gen[Frame[Row, Col]], coFrameGen.type, Gen[Frame[Row, Col]]]

  final class COFrameGenBuilder[Row, Col](rowKeyGen: Gen[Row]) {
    def apply[S, L <: HList](s: S)(implicit
        gen: Generic.Aux[S, L],
        folder: COFrameGenFolder[L, Row, Col],
        ctCol: ClassTag[Col], orderCol: Order[Col],
        ctRow: ClassTag[Row], orderRow: Order[Row]): Gen[Frame[Row, Col]] = {
      val frame0Gen = Gen.listOf(rowKeyGen).map { keys =>
        Frame.empty[Row, Col].withRowIndex(Index(keys.toArray))
      }
      gen.to(s).foldLeft(frame0Gen)(coFrameGen)
    }
  }

  final class ROFrameGenBuilder[Row, Col](colKeyGen: Gen[Col]) {
    def apply[S, L <: HList](s: S)(implicit
        gen: Generic.Aux[S, L],
        folder: COFrameGenFolder[L, Col, Row],
        ctCol: ClassTag[Col], orderCol: Order[Col],
        ctRow: ClassTag[Row], orderRow: Order[Row]): Gen[Frame[Row, Col]] =
      new COFrameGenBuilder[Col, Row](colKeyGen)(s).map(_.transpose)
  }

  def genColOrientedFrame[Row, Col](rowKeyGen: Gen[Row]) =
    new COFrameGenBuilder[Row, Col](rowKeyGen)

  def genRowOrientedFrame[Row, Col](colKeyGen: Gen[Col]) =
    new ROFrameGenBuilder[Row, Col](colKeyGen)
}

object FrameGenerators extends FrameGenerators
