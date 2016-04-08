package framian
package csv

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._
import spire.std.iterable._

import shapeless._
import java.io.{File, BufferedReader, FileReader}

class CsvSpec extends FramianSpec {
  val csvRoot = "framian/src/test/resources/csvs/"

  val airPassengers = csvRoot +"AirPassengers-test.csv"
  val airPassengersBadComma = csvRoot +"AirPassengers-badcomma.csv"
  val autoMPG = csvRoot +"auto-mpg-test.tsv"

  val defaultRowIndex = Index.fromKeys(0, 1, 2, 3, 4)
  val withColumnRowIndex = Index.fromKeys(0, 1, 2, 3)
  val defaultAPColumnIndex = Index.fromKeys(0, 1, 2)

  val defaultAirPassengers = ColOrientedFrame(
    Index.fromKeys(0, 1, 2, 3, 4),
    Series(
      0 -> TypedColumn(Column[Int](
          NA,
          Value(1),
          Value(2),
          Value(3),
          Value(4))
        ).orElse(TypedColumn(Column[String](
          Value("")
        ))),
      1 -> TypedColumn(Column[BigDecimal](
          NA,
          Value(BigDecimal("1949")),
          Value(BigDecimal("1949.08333333333")),
          Value(BigDecimal("1949.16666666667")),
          Value(BigDecimal("1949.25")))
        ).orElse(TypedColumn(Column[String](
          Value("time")
        ))),
      2 -> TypedColumn(Column[BigDecimal](
          NA,
          Value(BigDecimal("112")),
          Value(BigDecimal("118")),
          Value(BigDecimal("132")),
          Value(BigDecimal("129")))
        ).orElse(TypedColumn(Column[String](
          Value("AirPassengers")
        )))))

  val columnAirPassengers = Frame.fromRows(
    1 :: BigDecimal(1949)             :: 112 :: HNil,
    2 :: BigDecimal(1949.08333333333) :: 118 :: HNil,
    3 :: BigDecimal(1949.16666666667) :: 132 :: HNil,
    4 :: BigDecimal(1949.25)          :: 129 :: HNil)
    .withColIndex(Index.fromKeys("", "time", "AirPassengers"))
    .withRowIndex(withColumnRowIndex)

  val defaultMPG = Frame.fromRows(
    18.0 :: 8 :: 307.0 :: 130.0 :: 3504 :: 12.0 :: 70 :: 1 :: "chevrolet chevelle malibu" :: HNil,
    15.0 :: 8 :: 350.0 :: 165.0 :: 3693 :: 11.5 :: 70 :: 1 :: "buick skylark 320" :: HNil,
    18.0 :: 8 :: 318.0 :: 150.0 :: 3436 :: 11.0 :: 70 :: 1 :: "plymouth satellite" :: HNil,
    16.0 :: 8 :: 304.0 :: 150.0 :: 3433 :: 12.0 :: 70 :: 1 :: "amc rebel sst" :: HNil,
    17.0 :: 8 :: 302.0 :: 140.0 :: 3449 :: 10.5 :: 70 :: 1 :: "ford torino" :: HNil)
    .withRowIndex(defaultRowIndex)
    .withColIndex(Index.fromKeys(0, 1, 2, 3, 4, 5, 6, 7, 8))

  "CsvParser" should {
    import CsvCell._

    val TestFormat = CsvFormat(
      separator = ",",
      quote = "'",
      quoteEscape = "'",
      empty = "N/A",
      invalid = "N/M",
      header = false,
      rowDelim = CsvRowDelim.Custom("|"),
      allowRowDelimInQuotes = true
    )

    "parse air passengers as unlabeled CSV" in {
      Csv.parsePath(airPassengers).unlabeled.toFrame should === (defaultAirPassengers)
    }

    "parse air passengers as labeled CSV" in {
      Csv.parsePath(airPassengers).labeled.toFrame should === (columnAirPassengers)
    }

    "parse autoMPG as unlabeled TSV" in {
      Csv.parsePath(autoMPG).unlabeled.toFrame should === (defaultMPG)
    }

    "parse CSV with separator in quote" in {
      val data = """a,"b","c,d"|"e,f,g""""
      val csv = Csv.parseString(data, CsvFormat.Guess.withRowDelim("|"))
      val frame = csv.unlabeled.toFrame
      frame.getRow(0) should === (Some(Rec(0 -> "a", 1 -> "b", 2 -> "c,d")))
      frame[String](1, 0) should === (Value("e,f,g"))
      frame[String](1, 1) should === (NA)
      frame[String](1, 2) should === (NA)
    }

    "parse escaped quotes" in {
      Csv.parseString(
        "a,'''','c'''|'''''d''''', ''''",
        TestFormat
      ).rows should === (Vector(
        Right(CsvRow(Vector(Data("a"), Data("'"), Data("c'")))),
        Right(CsvRow(Vector(Data("''d''"), Data(" ''''")))))
      )
    }

    "respect CsvFormat separator" in {
      Csv.parseString("a,b,c|d,e,f", TestFormat).rows should === (
        Csv.parseString("a;b;c|d;e;f", TestFormat.withSeparator(";")).rows)
    }

    "respect CsvFormat quote" in {
      Csv.parseString("'a,b','b'|d,e", TestFormat).rows should === (
        Csv.parseString("^a,b^,^b^|d,e", TestFormat.withQuote("^")).rows)
    }

    "respect CsvFormat quote escape" in {
      Csv.parseString("'a''b',''''|' '", TestFormat).rows should === (
        Csv.parseString("'a\\'b','\\''|' '", TestFormat.withQuoteEscape("\\")).rows)
    }

    "respect CsvFormat empty" in {
      Csv.parseString("a,N/A,b|N/A,N/A", TestFormat).rows should === (
        Csv.parseString("a,,b|,", TestFormat.withEmpty("")).rows)
    }

    "respect CsvFormat invalid" in {
      Csv.parseString("a,N/M,b|N/M,N/M", TestFormat).rows should === (
        Csv.parseString("a,nm,b|nm,nm", TestFormat.withInvalid("nm")).rows)
    }

    "respect CsvFormat row delimiter" in {
      Csv.parseString("a,b|c,d|e,f", TestFormat).rows should === (
        Csv.parseString("a,b\nc,d\ne,f", TestFormat.withRowDelim(CsvRowDelim.Unix)).rows)
    }

    "parse CSV with row delimiter in quote" in {
      Csv.parseString("a,'b|c'|'d|e',f", TestFormat).rows should === (Vector(
        Right(CsvRow(Vector(Data("a"), Data("b|c")))),
        Right(CsvRow(Vector(Data("d|e"), Data("f"))))))
    }

    "parser respects whitespace" in {
      val data = " a , , 'a','b'|  b  ,c  ,   "
      val csv = Csv.parseString(data, CsvFormat.Guess.withRowDelim("|"))

      csv.rows should === (Vector(
        Right(CsvRow(Vector(Data(" a "), Data(" "), Data(" 'a'"), Data("b")))),
        Right(CsvRow(Vector(Data("  b  "), Data("c  "), Data("   "))))))
    }
  }
}
