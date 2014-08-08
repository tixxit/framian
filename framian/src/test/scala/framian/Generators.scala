package framian

import spire.algebra.Order

import scala.reflect.ClassTag

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

import scala.util.Random

trait CellGenerators {

  // Pre-determined ratios for the most common distributions for sparse and dirty columns and cells
  val dDense  = (1, 0)
  val dSparse = (9, 1)
  val dDirty  = (7, 2, 1)

  /** Equivalent to invoking {{{genCell(gen, (weight._1, weight._2, 0)}}}. It simply does not
    * generate [[NM]] values.
    *
    * @see [[genCell(Gen[A], (Int, Int, Int))]]
    */
  def genCell[A](gen: Gen[A], weight: (Int, Int)): Gen[Cell[A]] =
    genCell(gen, (weight._1, weight._2, 0))

  /** Generate a cell that can produce one of [[Value]], [[NA]] or [[NM]] cells based on a given
    * weighted probability distribution.
    *
    * @param gen    The generator that generates the inner values for the [[Value]] cell
    * @param weight The weighted probability that either a [[Value]] (`_1`), [[NA]] (`_2`) or [[NM]]
    *               (`_3`) will be generated. E.g., (1, 1, 1), leaves a 33% chance for each
    * @return       A generator that will produce the provided weighted distribution of cell types
    */
  def genCell[A](gen: Gen[A], weight: (Int, Int, Int)): Gen[Cell[A]] = {
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

trait ColumnGenerators extends CellGenerators {

  /** Generate a dense column, containing only [[Value]] cells.
    *
    * @param gen  The generator for the cell values
    * @return     The generator that will produce [[Column]]s
    */
  def genDenseColumn[A: ClassTag](gen: Gen[A]): Gen[Column[A]] = for {
    as <- Gen.nonEmptyListOf(gen)
  } yield Column.fromArray(as.toArray)

  /** Generate a sparse column, containing only [[Value]] and [[NA]] cells.
    *
    * @param gen  The generator for the cell values
    * @return     The generator that will produce [[Column]]s
    */
  def genSparseColumn[A](gen: Gen[A], weight: (Int, Int) = dSparse): Gen[Column[A]] = for {
    cells <- Gen.nonEmptyListOf(genCell(gen, weight))
  } yield Column.fromCells(cells.toVector)

  /** Generate a dirty column, containing any kind of cell, including all [[NonValue]] possibilities
    * and valid [[Value]]s with the given inner type.
    *
    * @param gen  The generator for the cell values
    * @return     The generator that will produce [[Column]]s
    */
  def genDirtyColumn[A](gen: Gen[A], weight: (Int, Int, Int) = dDirty): Gen[Column[A]] = for {
    cells <- Gen.nonEmptyListOf(genCell(gen, weight))
  } yield Column.fromCells(cells.toVector)
}
object ColumnGenerators extends ColumnGenerators

trait SeriesGenerators {

  /** Generates a [[Tuple2]] whose first and second element are generated from
    * the provided {{{firstGen}}} and {{{secondGen}}} generators.
    *
    * @param firstGen   The generator for the value of the first element of the [[Tuple2]]
    * @param secondGen  The generator for the value of the second element of the [[Tuple2]]
    * @return           The generator that will produce [[Tuple2]]s
    */
  def genTuple2[K, V](firstGen: Gen[K], secondGen: Gen[V]): Gen[(K, V)] = for {
    k <- firstGen
    v <- secondGen
  } yield (k, v)

  /** Generates a series that contains only [[NA]] elements or an empty series.
    *
    * @param keyGen The generator for the key of the [[NA]] values, if any
    * @return       The generator that will produce empty series
    */
  def genEmptySeries[K: Order: ClassTag, V: Arbitrary: ClassTag](keyGen: Gen[K]): Gen[Series[K, V]] =
    for {
      t2s <- Gen.listOf(genTuple2(keyGen, CellGenerators.genCell(arbitrary[V], (0, 1))))
    } yield Series.fromCells(t2s: _*)

  /** Generates a series of arbitrary keys that contains only [[NA]] elements or an empty series.
    *
    * @return   The generator that will produce empty series
    */
  def genEmptyArbitrarySeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag]: Gen[Series[K, V]] =
    genEmptySeries(arbitrary[K])

  /** Generates a series that only contains valid [[Value]] cells as values for the series.
    *
    * @param keyGen A generator that generates all the keys for the series index
    * @param valGen A generator that generates all the values wrapped in [[Value]]s
    * @return       A generator for a series that contains only [[Value]]s
    */
  def genNonEmptyDenseSeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V]): Gen[Series[K, V]] =
    for {
      t2s <- Gen.nonEmptyListOf(genTuple2(keyGen, CellGenerators.genCell(valGen, CellGenerators.dDense)))
    } yield Series.fromCells(t2s: _*)

  /** Generates a series that contains arbitrary values for both key and value according to the
    * given types {{{K}}} and {{{V}}}.
    *
    * @return       A generator for a series that contains only arbitrary [[Value]]s
    */
  def genNonEmptyArbitraryDenseSeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag]: Gen[Series[K, V]] =
    genNonEmptyDenseSeries(arbitrary[K], arbitrary[V])

  /** Generates a series that contains both [[Value]] and [[NA]] cells as values.
    *
    * @param keyGen A generator that generates all the keys for the series index
    * @param valGen A generator that generates all the values wrapped in [[Value]]s
    * @return       A generator for a series that contains [[Value]]s and [[NA]]s
    */
  def genNonEmptySparseSeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V],
    weight: (Int, Int) = CellGenerators.dSparse): Gen[Series[K, V]] =
    for {
      denseKey <- keyGen
      denseCell <- CellGenerators.genCell(valGen, CellGenerators.dDense)
      sparseKey <- keyGen
      sparseCell <- CellGenerators.genCell(valGen, (0, 1))
      t2s <- Gen.listOf(genTuple2(keyGen, CellGenerators.genCell(valGen, weight)))
    } yield
      // Add 2-tuples that are known to be dense and sparse to any list generated by the listOf
      // generator. This ensures we always have at least one dense cell and one sparse cell in the
      // series
      Series.fromCells(Random.shuffle((denseKey, denseCell) :: (sparseKey, sparseCell) :: t2s): _*)

  /** Generates a series that contains arbitrary values for both key and value according to the
    * given types {{{K}}} and {{{V}}}.
    *
    * @return       A generator for a series that contains arbitrary [[Value]]s and [[NM]]s
    */
  def genNonEmptyArbitrarySparseSeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag](
    weight: (Int, Int) = CellGenerators.dSparse): Gen[Series[K, V]] =
    genNonEmptySparseSeries(arbitrary[K], arbitrary[V], weight)

  /** Generates a series that contains any potential cell type as values, including [[Value]],
    * [[NA]] and [[NM]] types.
    *
    * @param keyGen  A generator that generates all the keys for the series index
    * @param valGen  A generator that generates all the values wrapped in [[Value]]s
    * @return        A generator for a series that contains [[Value]]s, [[NA]]s and [[NM]]s
    */
  def genNonEmptyDirtySeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V],
    weight: (Int, Int, Int) = CellGenerators.dDirty): Gen[Series[K, V]] =
    for {
      denseKey <- keyGen
      denseCell <- CellGenerators.genCell(valGen, CellGenerators.dDense)
      sparseKey <- keyGen
      sparseCell <- CellGenerators.genCell(valGen, (0, 1))
      dirtyKey <- keyGen
      dirtyCell <- CellGenerators.genCell(valGen, (0, 0, 1))
      t2s <- Gen.listOf(genTuple2(keyGen, CellGenerators.genCell(valGen, weight)))
    } yield
      // Add 2-tuples that are known to be dense, sparse, and dirty to any list generated by the
      // listOf generator. This ensures we always have at least one of each cell in the series
      Series.fromCells(Random.shuffle((denseKey, denseCell) :: (sparseKey, sparseCell) ::
        (dirtyKey, dirtyCell) :: t2s): _*)

  /** Generates a series that contains arbitrary values for both key and value according to the
    * given types {{{K}}} and {{{V}}}.
    *
    * @return       A generator for a series that contains arbitrary [[Value]]s, [[NA]]s and [[NM]]s
    */
  def genNonEmptyArbitraryDirtySeries[K: Arbitrary: Order: ClassTag, V: Arbitrary: ClassTag](
    weight: (Int, Int, Int) = CellGenerators.dDirty): Gen[Series[K, V]] =
    genNonEmptyDirtySeries(arbitrary[K], arbitrary[V], weight)
}
object SeriesGenerators extends SeriesGenerators