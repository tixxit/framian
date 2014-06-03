package pellucid.pframe

sealed trait Rows[K, A] extends AxisSelectionLike[K, A, Rows] {
  def toCols: Cols[K, A] = this match {
    case Rows.All(e) => Cols.All(e)
    case Rows.Pick(keys, e) => Cols.Pick(keys, e)
  }
}

object Rows extends AxisSelectionCompanion[Rows] {
  case class All[K, A](extractor: RowExtractor[A, K, Variable]) extends Rows[K, A] with AllAxisSelection[K, A]
  object All extends AllCompanion

  case class Pick[K, S <: Size, A](keys: List[K], extractor: RowExtractor[A, K, S]) extends Rows[K, A] with PickAxisSelection[K, S, A]
  object Pick extends PickCompanion
}
