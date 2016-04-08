package framian

class UntypedColumnSpec extends FramianSpec {
  "ConcatColumn" should {
    "be stable after reindex" in {
      val col1 = TypedColumn(Column.dense(Array(1, 2)))
      val col2 = TypedColumn(Column.dense(Array(3, 4)))
      val col3 = ConcatColumn(col1, col2, 2)
      val col = col3.reindex(Array(0,1,2,3)).cast[Int]
      (0 to 4).map(col(_)) shouldBe Seq(Value(1), Value(2), Value(3), Value(4), NA)
    }
  }

  "cast" should {
    "return NMs when values don't make sense" in {
      val col0 = TypedColumn(Column(Value(42), NA, NM, Value(32))).cast[String]
      col0(0) should === (NM)
      col0(1) should === (NA)
      col0(2) should === (NM)
      col0(3) should === (NM)
      col0(4) should === (NA)

      val col1 = TypedColumn(Column(Value("x"), NA, NM, Value("y"))).cast[Double]
      col1(0) should === (NM)
      col1(1) should === (NA)
      col1(2) should === (NM)
      col1(3) should === (NM)
      col1(4) should === (NA)
    }
  }
}
