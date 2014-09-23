package framian
package column

private[framian] case class EvalColumn[A](f: Int => Cell[A]) extends BoxedColumn[A] {
  override def apply(row: Int): Cell[A] = f(row)

  def cellMap[B](g: Cell[A] => Cell[B]): Column[B] = EvalColumn(f andThen g)

  def reindex(index: Array[Int]): Column[A] =
    DenseColumn.force(index andThen f, index.length)

  def force(len: Int): Column[A] =
    DenseColumn.force(f, len)

  def mask(mask: Mask): Column[A] = EvalColumn { row =>
    if (mask(row)) NA else f(row)
  }

  def setNA(naRow: Int): Column[A] = EvalColumn { row =>
    if (row == naRow) NA else f(row)
  }

  def memoize(optimistic: Boolean): Column[A] =
    if (optimistic) new OptimisticMemoizingColumn(f)
    else new PessimisticMemoizingColumn(f)

  def orElse[A0 >: A](that: Column[A0]): Column[A0] =
    EvalColumn { row =>
      f(row) match {
        case NA => that(row)
        case cell => cell
      }
    }

  def shift(n: Int): Column[A] = EvalColumn(row => f(row - n))
}

