package framian
package utilities

import org.specs2.mutable._

import spire.algebra._
import spire.std.string._
import spire.std.double._
import spire.std.int._
import spire.std.iterable._

import shapeless._
import java.io.{File, BufferedReader, FileReader}

class CsvSpec extends Specification {
  val csvRoot = "framian/src/test/resources/csvs/"

  val airPassengers = csvRoot +"AirPassengers-test.csv"
  val airPassengersBadComma = csvRoot +"AirPassengers-badcomma.csv"
  val autoMPG = csvRoot +"auto-mpg-test.tsv"

  val defaultRowIndex = Index.fromKeys("0", "1", "2", "3", "4")
  val withColumnRowIndex = Index.fromKeys("0", "1", "2", "3")
  val defaultAPColumnIndex = Index.fromKeys("0", "1", "2")

  val defaultAirPassengers = Frame.fromGeneric(
    "" :: "time" :: "AirPassengers" :: HNil,
    "1" :: "1949" :: "112" :: HNil,
    "2" :: "1949.08333333333" :: "118" :: HNil,
    "3" :: "1949.16666666667" :: "132" :: HNil,
    "4" :: "1949.25" :: "129" :: HNil)
    .withColIndex(defaultAPColumnIndex)
    .withRowIndex(defaultRowIndex)
  val columnAirPassengers = Frame.fromGeneric(
    "1" :: "1949" :: "112" :: HNil,
    "2" :: "1949.08333333333" :: "118" :: HNil,
    "3" :: "1949.16666666667" :: "132" :: HNil,
    "4" :: "1949.25" :: "129" :: HNil)
    .withColIndex(Index.fromKeys("", "time", "AirPassengers"))
    .withRowIndex(withColumnRowIndex)
  val rowAirPassengers = (Frame.fromGeneric(
    "time" :: "AirPassengers" :: HNil,
    "1949" :: "112" :: HNil,
    "1949.08333333333" :: "118" :: HNil,
    "1949.16666666667" :: "132" :: HNil,
    "1949.25" :: "129" :: HNil)
    .withColIndex(Index.fromKeys("0", "1"))
    .withRowIndex(Index.fromKeys("", "1", "2", "3", "4")))
  val correctAirPassengers = Frame.fromGeneric(
    "1949" :: "112" :: HNil,
    "1949.08333333333" :: "118" :: HNil,
    "1949.16666666667" :: "132" :: HNil,
    "1949.25" :: "129" :: HNil)
    .withColIndex(Index.fromKeys("time", "AirPassengers"))
    .withRowIndex(Index.fromKeys("1", "2", "3", "4"))

  val defaultMPG = Frame.fromGeneric(
    "18.0" :: "8" :: "307.0" :: "130.0" :: "3504." :: "12.0" :: "70" :: "1" :: "chevrolet chevelle malibu" :: HNil,
    "15.0" :: "8" :: "350.0" :: "165.0" :: "3693." :: "11.5" :: "70" :: "1" :: "buick skylark 320" :: HNil,
    "18.0" :: "8" :: "318.0" :: "150.0" :: "3436." :: "11.0" :: "70" :: "1" :: "plymouth satellite" :: HNil,
    "16.0" :: "8" :: "304.0" :: "150.0" :: "3433." :: "12.0" :: "70" :: "1" :: "amc rebel sst" :: HNil,
    "17.0" :: "8" :: "302.0" :: "140.0" :: "3449." :: "10.5" :: "70" :: "1" :: "ford torino" :: HNil)
    .withRowIndex(defaultRowIndex)
    .withColIndex(Index.fromKeys("0", "1", "2", "3", "4", "5", "6", "7", "8"))
  val withRowIndexMPG = Frame.fromGeneric(
    "18.0" :: "8" :: "307.0" :: "130.0" :: "3504." :: "12.0" :: "70" :: "1" :: HNil,
    "15.0" :: "8" :: "350.0" :: "165.0" :: "3693." :: "11.5" :: "70" :: "1" :: HNil,
    "18.0" :: "8" :: "318.0" :: "150.0" :: "3436." :: "11.0" :: "70" :: "1" :: HNil,
    "16.0" :: "8" :: "304.0" :: "150.0" :: "3433." :: "12.0" :: "70" :: "1" :: HNil,
    "17.0" :: "8" :: "302.0" :: "140.0" :: "3449." :: "10.5" :: "70" :: "1" :: HNil)
    .withRowIndex(Index.fromKeys(
                    "chevrolet chevelle malibu", "buick skylark 320",
                    "plymouth satellite", "amc rebel sst", "ford torino"))
    .withColIndex(Index.fromKeys("0", "1", "2", "3", "4", "5", "6", "7"))
  val customColsMPG = Frame.fromGeneric(
    "18.0" :: "8" :: "307.0" :: HNil,
    "15.0" :: "8" :: "350.0" :: HNil,
    "18.0" :: "8" :: "318.0" :: HNil,
    "16.0" :: "8" :: "304.0" :: HNil,
    "17.0" :: "8" :: "302.0" :: HNil)
    .withRowIndex(defaultRowIndex)
    .withColIndex(Index.fromKeys("0", "1", "2"))
  val withRowIndexCustomColsMPG = Frame.fromGeneric(
    "18.0" :: "8" :: "307.0" :: HNil,
    "15.0" :: "8" :: "350.0" :: HNil,
    "18.0" :: "8" :: "318.0" :: HNil,
    "16.0" :: "8" :: "304.0" :: HNil,
    "17.0" :: "8" :: "302.0" :: HNil)
    .withRowIndex(Index.fromKeys(
                    "chevrolet chevelle malibu", "buick skylark 320",
                    "plymouth satellite", "amc rebel sst", "ford torino"))
    .withColIndex(Index.fromKeys("0", "1", "2"))

  val apBadComma = Frame.fromGeneric(
    "" :: "FlightName" :: "AirPassengers" :: HNil,
    "1" :: "ABCD111" :: "112" :: HNil,
    "2" :: "Delta20394" :: "118" :: HNil,
    "3" :: "FLIGHTTOHELL, REALLY" :: "132" :: HNil,
    "4" :: "United666" :: "129" :: HNil)
    .withColIndex(defaultAPColumnIndex)
    .withRowIndex(defaultRowIndex)

  def getFile(loc:String) = new File(loc)

  "Csv parser" should {
    "parse air passengers with default settings" in {
        loadFrameFromCSV(getFile(airPassengers)) must_== defaultAirPassengers
      }

    "parse air passengers with just column headers" in {
      loadFrameFromCSV(getFile(airPassengers), columnIndex = true) must_== columnAirPassengers
    }

    "parse air passengers with just row headers" in {
      loadFrameFromCSV(getFile(airPassengers), columnIndex = false, rowIndex = 0) must_== rowAirPassengers
    }

    "parse air passengers with row and column headers" in {
      loadFrameFromCSV(getFile(airPassengers), columnIndex = true, rowIndex = 0) must_== correctAirPassengers
    }

    "fail to parse autoMPG with default settings" in {
      loadFrameFromCSV(getFile(autoMPG)) must throwA[AssertionError]
    }

    "parse autoMPG with delimiter as tab but otherwise default settings" in {
      loadFrameFromCSV(getFile(autoMPG), delimiter = "\t") must_== defaultMPG
    }

    "parse autoMPG with delimiter as tab and row index as column 8" in {
      loadFrameFromCSV(getFile(autoMPG), delimiter = "\t", rowIndex = 8) must_== withRowIndexMPG
    }

    "parse autoMPG with delimiter as tab and just take first three columns" in {
      loadFrameFromCSV(getFile(autoMPG), delimiter = "\t", columns = List(0, 1, 2)) must_== customColsMPG
    }

    "parse autoMPG with delimiter as tab, column 8 as row index and just take first three columns" in {
      loadFrameFromCSV(getFile(autoMPG), delimiter = "\t", rowIndex = 8, columns = List(0, 1, 2)) must_== withRowIndexCustomColsMPG
    }

    "parse a file that has the delimiter within a column within the chosen quote type" in {
      loadFrameFromCSV(getFile(airPassengersBadComma)) must_== apBadComma
    }
  }
}
