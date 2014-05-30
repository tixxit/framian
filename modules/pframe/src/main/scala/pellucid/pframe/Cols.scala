package pellucid.pframe

sealed trait Cols[K, A] extends AxisSelectionLike[K, A, Cols]
object Cols extends AxisSelectionCompanion {
  type AxisSelection[K, A] = Cols[K, A]

  case class All[K, A](extractor: RowExtractor[A, K, Variable]) extends Cols[K, A] with AllAxisSelection[K, A]
  object All extends AllCompanion

  case class Pick[K, S <: Size, A](keys: List[K], extractor: RowExtractor[A, K, S]) extends Cols[K, A] with PickAxisSelection[K, S, A]
  object Pick extends PickCompanion
}
