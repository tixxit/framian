package framian

import scala.reflect.ClassTag

import org.scalacheck._

trait CellGenerators {
  def genCell[A](gen: Gen[A]): Gen[Cell[A]] =
    Gen.oneOf(Gen.const(NA), Gen.const(NM), gen map (Value(_)))
}

object CellGenerators extends CellGenerators

trait ColumnGenerators extends CellGenerators {
  def genDenseColumn[A: ClassTag](gen: Gen[A]): Gen[Column[A]] = for {
    as <- Gen.listOf(gen)
  } yield Column.fromArray(as.toArray)

  def genSparseColumn[A](gen: Gen[A]): Gen[Column[A]] = for {
    cells <- Gen.listOf(genCell(gen))
  } yield Column.fromCells(cells.toVector)
}

object ColumnGenerators extends ColumnGenerators
