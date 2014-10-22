package framian

import org.specs2.mutable.Specification

class UntypedColumnSpec extends Specification {
  "ConcatColumn" should {
    "be stable after reindex" in {
      val col1 = TypedColumn(Column.fromArray(Array(1, 2)))
      val col2 = TypedColumn(Column.fromArray(Array(3, 4)))
      val col3 = ConcatColumn(col1, col2, 2)
      val col = col3.reindex(Array(0,1,2,3)).cast[Int]
      (0 to 4).map(col(_)) must_== Seq(Value(1), Value(2), Value(3), Value(4), NA)
    }
  }
}
