package framian

import spire.algebra.Order

import scala.reflect.ClassTag

import org.scalacheck._

trait CellGenerators {


  /** Generate a cell that is only a valid [[Value]] cell that contains the value produced by the
    * given generator.
    *
    * @param gen   The generator for the cell value
    * @return      The generator that will produce only [[Value]]s
    */
  def genDenseCell[A](gen: Gen[A]): Gen[Cell[A]] =
    gen.map(Value(_))

  /** Generate a cell that will either be a [[Value]] or [[NA]].
    *
    * @param gen  The generator for cell values
    * @return     The generator that will produce [[Value]] or [[NA]] cells
    */
  def genSparseCell[A](gen: Gen[A]): Gen[Cell[A]] =
    Gen.oneOf(Gen.const(NA), gen.map(Value(_)))

  /** Generate a cell that is one of any possible cell kind, including all [[NonValue]]
    * possibilities and valid [[Value]]s with the given inner type.
    *
    * @param gen  The generator for cell values
    * @return     The generator that will produce all types of cells
    */
  def genDirtyCell[A](gen: Gen[A]): Gen[Cell[A]] =
    Gen.oneOf(Gen.const(NA), Gen.const(NM), gen.map(Value(_)))
}
object CellGenerators extends CellGenerators

trait ColumnGenerators extends CellGenerators {

  /** Generate a dense column, containing only [[Value]] cells.
    *
    * @param gen  The generator for the cell values
    * @return     The generator that will produce [[Column]]s
    */
  def genDenseColumn[A: ClassTag](gen: Gen[A]): Gen[Column[A]] = for {
    as <- Gen.listOf(gen)
  } yield Column.fromArray(as.toArray)

  /** Generate a sparse column, containing only [[Value]] and [[NA]] cells.
    *
    * @param gen  The generator for the cell values
    * @return     The generator that will produce [[Column]]s
    */
  def genSparseColumn[A](gen: Gen[A]): Gen[Column[A]] = for {
    cells <- Gen.listOf(genSparseCell(gen))
  } yield Column.fromCells(cells.toVector)

  /** Generate a dirty column, containing any kind of cell, including all [[NonValue]] possibilities
    * and valid [[Value]]s with the given inner type.
    *
    * @param gen  The generator for the cell values
    * @return     The generator that will produce [[Column]]s
    */
  def genDirtyColumn[A](gen: Gen[A]): Gen[Column[A]] = for {
    cells <- Gen.listOf(genDirtyCell(gen))
  } yield Column.fromCells(cells.toVector)
}
object ColumnGenerators extends ColumnGenerators

trait SeriesGenerators extends CellGenerators {

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

  /** Generates a series that only contains valid [[Value]] cells as values for the series.
    *
    * @param keyGen A generator that generates all the keys for the series index
    * @param valGen A generator that generates all the values wrapped in [[Value]]s
    * @return       A generator for series that contain no [[NonValue]] values
    */
  def genDenseSeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V]): Gen[Series[K, V]] =
    for {
      t2s <- Gen.listOf(genTuple2(keyGen, genDenseCell(valGen)))
    } yield Series.fromCells(t2s: _*)

  /** Generates a series that contains both [[Value]] and [[NA]] cells as values.
    *
    * @param keyGen A generator that generates all the keys for the series index
    * @param valGen A generator that generates all the values wrapped in [[Value]]s
    * @return       A generator for series that contain [[Value]] and [[NA]] values
    */
  def genSparseSeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V]): Gen[Series[K, V]] =
    for {
      t2s <- Gen.listOf(genTuple2(keyGen, genSparseCell(valGen)))
    } yield Series.fromCells(t2s: _*)

  /** Generates a series that contains any potential cell type as values, including [[Value]],
    * [[NA]] and [[NM]] types.
    *
    * @param keyGen  A generator that generates all the keys for the series index
    * @param valGen  A generator that generates all the values wrapped in [[Value]]s
    * @return        A generator for series that contain [[Value]] and [[NA]] values
    */
  def genDirtySeries[K: Order: ClassTag, V: ClassTag](keyGen: Gen[K], valGen: Gen[V]): Gen[Series[K, V]] =
    for {
      t2s <- Gen.listOf(genTuple2(keyGen, genDirtyCell(valGen)))
    } yield Series.fromCells(t2s: _*)
}
object SeriesGenerators extends SeriesGenerators