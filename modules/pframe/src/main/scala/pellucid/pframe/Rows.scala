package pellucid.pframe

sealed trait Rows[K, A] extends AxisSelectionLike[K, A, Rows]
object Rows extends AxisSelectionCompanion[Rows] {
  case class All[K, A](extractor: RowExtractor[A, K, Variable]) extends Rows[K, A] with AllAxisSelection[K, A]
  object All extends AllCompanion

  case class Pick[K, S <: Size, A](keys: List[K], extractor: RowExtractor[A, K, S]) extends Rows[K, A] with PickAxisSelection[K, S, A]
  object Pick extends PickCompanion
}
