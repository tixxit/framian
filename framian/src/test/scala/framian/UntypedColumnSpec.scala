package framian

import org.specs2.mutable.Specification

class UntypedColumnSpec extends Specification {
  "ConcatColumn" should {
    "be stable after reindex" in {
      val col1 = TypedColumn(Column.dense(Array(1, 2)))
      val col2 = TypedColumn(Column.dense(Array(3, 4)))
      val col3 = ConcatColumn(col1, col2, 2)
      val col = col3.reindex(Array(0,1,2,3)).cast[Int]
      (0 to 4).map(col(_)) must_== Seq(Value(1), Value(2), Value(3), Value(4), NA)
    }
  }

  "cast" should {
    "return NMs when values don't make sense" in {
      val col0 = TypedColumn(Column(Value(42), NA, NM, Value(32))).cast[String]
      col0(0) must_== NM
      col0(1) must_== NA
      col0(2) must_== NM
      col0(3) must_== NM
      col0(4) must_== NA

      val col1 = TypedColumn(Column(Value("x"), NA, NM, Value("y"))).cast[Double]
      col1(0) must_== NM
      col1(1) must_== NA
      col1(2) must_== NM
      col1(3) must_== NM
      col1(4) must_== NA
    }
  }
}
