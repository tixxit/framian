Getting Started
===============

This tutorial is best if you follow along with me, trying the examples. To get
started, [clone the tutorial repo][Tutorial Repo], and start up a Scala REPL
from the root of the project with `sbt console`:

```
$ git clone https://github.com/tixxit/framian-tutorial.git
$ cd framian-tutorial
$ sbt console

Then, import some things before we get started:
```tut
import framian._
import framian.csv.Csv
import java.time.LocalDate
import spire.implicits._
```

Great! We'll just be using the REPL in this tutorial. Aside from bringing in
some dependencies and some imports, this also adds some utility functions
that'll let us fetch company information and share price data from Yahoo!
Finance.

Among the imports we see above, there are some obvious ones, like `framian.\_`
(this a tutorial on Framian, after all), but less obvious is
`spire.implicits.\_`. [Spire][Spire] is a Scala library that provides many
useful abstractions for working with *numbers* (and things that sort of behave
like numbers). Framian uses Spire so it can abstract its operations over the
actual number type used. This means you choose the number type, whether that is
`Double`, `BigDecimal`, or perhaps something more exotic like
`spire.math.Rational` or `spire.math.Number`. The `implicits` import
brings in the necessary implicit instances required to use Spire's
abstractions.

Making our First "Frame"
========================

Framian was made to help us easily work with tabular, heterogeneous data. In
Framian, we call one of these heterogeneous tables a **frame** and it is
represented by the [`Frame`][Frame]. So, we'll start by making some frames.

`Frame` provides a number of constructors, but we'll start with one that builds
a frame from a list of case class instances. So, first, we can model a company
as a case class.

```tut
case class Company(name: String, exchange: String, ticker: String, currency: String, marketCap: BigDecimal)
```

This case class includes some basic information we may find useful later, such
as the company's reporting currency, their exchange, ticker, and a human
readable name. Let's create a few fake companies to populate a frame with.

```tut
val Acme = Company("Acme", "NASDAQ", "ACME", "USD", BigDecimal("123.00"))

val BobCo = Company("Bob Company", "NASDAQ", "BOB", "USD", BigDecimal("45.67"))

val Cruddy = Company("Cruddy Inc", "XETRA", "CRUD", "EUR", BigDecimal("1.00"))
```

And now we create the frame using `Frame.fromRows`.

```tut
val frameFromRows = Frame.fromRows(Acme, BobCo, Cruddy)
```

Great! The `fromRows` constructor will work with any simple case class, tuples
or Shapeless `HList`s. These values are transformed into tabular form by creating
a row for each value, and a column for each field in the case class or tuple.

You'll notice that along the left side and top of our data frame, we see some
numbers that label the rows and columns. These are the `Frame`'s row and column
*indexes*. They define the primary way of selecting and manipulating rows,
columns, and groups of rows or columns in a `Frame`. They also don't have to be
`Int` - the type of our frame isn't just `Frame`, but `Frame[Int, Int]`. Those
2 type parameters define the type of our row and column index respectively.

Having our company frame's columns indexed by `Int`s is silly, when we can give
them perfectly good names. A `Frame` has many ways of changing how we index the
columns or rows. For now, let's create a custom index for them by mapping each
column index to a String.

```tut
val colKeysFrame = frameFromRows.mapColKeys {
  case 0 => "Name"
  case 1 => "Exchange"
  case 2 => "Ticker"
  case 3 => "Currency"
  case 4 => "Market Cap"
}
```

Here we used `Frame`'s `mapColKeys` method. This keeps the columns in the
original order and simply changes the keys value. You'll see our result now has
type `Frame[Int,String]`, since our column keys are now strings!

OK, great... so how do we actually use these fancy new column keys to access
the data? Let's start by trying to use the ticker symbol as the row keys.
Rather then using `mapRowKeys` like we did above, we'd like to use an approach
that uses the ticker symbol that already exists as data in the frame.

In Framian, a `Frame` doesn't know anything about the *type* of the data stored
in it. Rather, it depends on the user to know these types of details. When we
access data in a frame, we must provide a way to extract the data we want as the
type we want. We do this by using [`Cols`][Cols] and [`Rows`][Rows].

Almost all methods on `Frame` that manipulate the data in any meaningful way 
will have at least one `Cols` or `Rows` argument. The choice of `Cols` or `Rows`
defines the axis we are selecting along. In the case of our ticker symbol, we
know that it is in the column `"Ticker"` and that it is a `String`. So, let's
define a `Cols` that can extract our tickers.

```tut
val ticker = Cols("Ticker").as[String]
```

When we construct a `Cols` instance using `Cols("Ticker")`, it will extract
each ticker symbol as a [`Rec`][Rec]. A `Rec` is rarely what we want, so we
use the `as[A]` method to tell the `Cols` instance to extract the column as
a type `A`.

Using `ticker`, we can now reindex our rows using the ticker symbol, instead
of the row index.

```tut
val frame = colKeysFrame.reindex(ticker)
```

Now we're getting somewhere. We have a frame with basic company information,
where each company is indexed by that company's stock ticker and each field
is indexed by the field name. Let's see how we can work with some of the data
in this frame.

Working With Data in a Frame
============================

Cells
-----

We can get a single cell out of the frame by using `Frame`'s `apply` method.
This method requires a type, the row key and the column key, and will return a
single cell from the frame. Remember, a frame doesn't know anything about the
*type* of data it contains, so the type parameter *must* be supplied.

```tut
frame[BigDecimal]("ACME", "Market Cap")
```

The first thing we can notice is that we didn't get back a value of type
`BigDecimal` - we got a value of type `Cell[BigDecimal]`. This is because
Framian does not assume your data is *dense*. A cell in a frame *may* contain
data, but it may also be *missing* or be *invalid*. [`Cell`][Cell] is a data
type that has 3 cases (sub-classes):

 * [`Value(value)`][Value] - the cell's data exists and is valid,
 * [`NA`][NA] - the cell's data is missing or *not available*, and
 * [`MM`][NM] - the cell's data is invalid or *not meaningful*.

Whenever we extract a value out of a frame we will actually be working with
`Cell`s instead. You can think of `Cell` like Scala's `Option`, except we have
2 cases of missing/invalid data (`NA` and `NM`), instead of just 1 (`None`).

Aside from getting back a cell, you'll also note that we also requested the
value as a `BigDecimal`. What would happen if we asked for it as something
else? Let's try!

```tut
frame[Double]("BOB", "Market Cap")
```

You'll notice that for Acme, we asked for the cell as type `BigDecimal`, but
for Bob Co, we asked for the market cap as a `Double`. Both returned a value.
In Framian, we support conversions between most common numeric types found in
Scala and Spire. Framian attempts to keep numbers abstract, letting the user
decide what kind of precision/speed trade-off they want to make, rather than
forcing 1 type. Even though we had *stored* the data as a `BigDecimal`, when we
requested it as a `Double` Framian performed the conversion for us.

OK, well, what if we ask for a less sensible type?

```tut
frame[LocalDate]("CRUD", "Market Cap")
```

We got back an `NM`, which indicates that the data is invalid or not
meaningful. This makes sense, since we can not meaningfully convert a decimal
number to a `LocalDate` (a `LocalDate` is just a product of year, month and
day, like 2014-10-31).

Series
------

Working with individual values has its uses, but we usually want to work with
an entire subset of the frame instead, such as a group of columns or rows. We
do this using the same `Cols`/`Rows` mechanism described above. [`Cols`][Cols]
(or [`Rows`][Rows]) describe some selection of columns (or rows), along with a
their type. Previously, we had defined `ticker` as `Cols("Ticker").as[String]`
and used it to reindex the frame by the companies' tickers. Let's define
another one to extract the market cap.

```tut
val marketCap = Cols("Market Cap").as[BigDecimal]
```

We can use `marketCap` to extract a [`Series`][Series] from the frame.

```tut
frame.get(marketCap)
```

Now we have the market caps as a *series* of numbers, *indexed* by their
tickers. `Series` are how we work with *typed*, 1-dimensional. `Cell`s are typed
too, but have no axis, so are 0-dimensional (just a value). A `Frame` is
2-dimensional (has 2 axes), but is *untyped*. In Scala, we require a type to do
pretty much anything useful with a value, so you will find much of your work
with frames will require converting subsets of columns and rows to `Series`
first.

Much like `Frame`s, `Series` also don't assume the data is dense. Series are
actually an indexed, list of *cells*, rather than values. In the case of market
cap, the data is dense, so everything is wrapped in [`Value`][Value]. If any of
the data were missing or invalid, we would see `NA`s and `NM`s in the output
above.

Before we go further, let's work a slightly more interesting data set than 3
fake companies. The framian-tutorial project includes some nice utility methods
that will fetch basic data from Yahoo! Finance for us and stuff 'em into
frames. Let's fetch some basic company information about a few car companies.

```tut:silent
object tutorial {

  /**
   * Parses a value with an optional "scale" suffix at the end, such as 123K,
   * 24M, or 3.12B. This will return the scaled `BigDecimal`. Valid scales are
   * currently K, M, B, or T, for 1,000, 1,000,000, 1,000,000,000, and
   * 1,000,000,000,000 respectively.
   *
   * @param value the scaled value string
   */
  def parseScaledAmount(value: String): Cell[BigDecimal] = value.toUpperCase match {
    case ScaledAmount(value, scale) => Value(BigDecimal(value) * getScale(scale))
    case _ => NM
  }

  /**
   * Fetch some basic company info for the given IDs. This returns a frame with
   * the following columns: Name, Stock Exchange, Ticker, Currency, and Market
   * Cap. There is 1 row per company.
   *
   * @param ids a list of Yahoo! IDs to fetch company information for
   */
  def fetchCompanyInfo(ids: String*): Frame[Int, String] = {
    val idList = ids.mkString(",")
    val fields = "nxsc4j1"
    val csv = fetchCsv(s"http://download.finance.yahoo.com/d/quotes.csv?s=$idList&f=$fields&e=.csv")
    csv.unlabeled.toFrame.withColIndex(Index.fromKeys("Name", "Stock Exchange", "Ticker", "Currency", "Market Cap"))
  }

  /**
   * Fetches the last 5 years of share price data, given a id, from Yahoo!
   * Finance.
   *
   * @param id a valid Yahoo! Finance ID
   */
  def fetchSharePriceData(id: String): Frame[Int, String] =
    fetchSharePriceData(id, LocalDate.now().minusYears(5), LocalDate.now())

  def fetchSharePriceData(id1: String, id2: String, id3: String*): Frame[Int, String] = {
    val ids = id1 :: id2 :: id3.toList
    ids.map { id =>
      val frame = fetchSharePriceData(id)
      val names = Series(frame.rowIndex, Column.value(id))
      frame.join("Ticker", names)(Join.Inner)
    }.reduceLeft(_ appendRows _)
  }

  /**
   * Fetches some historical share price data from Yahoo! Finance. The result
   * is returned as a CSV file, so we use Framian's CSV parser to convert it to
   * a frame.
   *
   * @param id     a valid Yahoo! Finance ID
   * @param start  the date to start fetching data from
   * @param end    the date to fetch data to (inclusive)
   */
  def fetchSharePriceData(id: String, start: LocalDate, end: LocalDate): Frame[Int, String] = {
    val paramMap = Map(
      "s" -> id,
      "a" -> (start.getMonthValue - 1),
      "b" -> start.getDayOfMonth,
      "c" -> start.getYear,
      "d" -> (end.getMonthValue - 1),
      "e" -> end.getDayOfMonth,
      "f" -> end.getYear,
      "ignore" -> ".csv")
    val queryString = paramMap.map { case (k, v) => s"$k=$v" }.mkString("&")
    val csv = fetchCsv(s"http://real-chart.finance.yahoo.com/table.csv?$queryString")
    csv.labeled.toFrame
  }

  /**
   * Fetches the exchange rate data from Yahoo! Finance. All rates are for
   * conversions to USD.
   *
   * @param currencies a list of currencies (eg. CAD, EUR, etc)
   */
  def fetchExchangeRate(currencies: String*): Frame[Int, String] = {
    val ids = currencies.map(_ + "USD=X").mkString(",")
    val fields = "c4l1"
    val csv = fetchCsv(s"http://download.finance.yahoo.com/d/quotes.csv?s=$ids&f=$fields&e=.csv")
    csv.unlabeled.toFrame.withColIndex(Index.fromKeys("Currency", "Rate"))
  }

  private def fetchCsv(url: String): Csv = {
    val csv = scala.io.Source.fromURL(url).mkString
    Csv.parseString(csv)
  }

  private val ScaledAmount = """(\d+(?:\.\d+))([kKmMbB])?""".r

  private def getScale(suffix: String): BigDecimal = suffix match {
    case "k" | "K" => BigDecimal(1000)
    case "m" | "M" => BigDecimal(1000000)
    case "b" | "B" => BigDecimal(1000000000)
    case "t" | "T" => BigDecimal("1000000000000")
  }
}

import tutorial._
```

```tut
val fetched = fetchCompanyInfo("GM", "HMC", "BMW.DE")
```

This looks very similar to our previous frame... almost like it was planned
that way. Well, let's get this reindexed by ticker.

```tut
val companies = fetched.reindex(ticker)
```

That's great. We can also fetch their share price data for the last 5 years.

```tut
val pricing = fetchSharePriceData("GM", "HMC", "BMW.DE").reindex(ticker)
```

We also reindexed the frame by the ticker right away.

[Tutorial Repo]: https://github.com/tixxit/framian-tutorial/
[Spire]: https://github.com/non/spire
[Frame]: https://tixxit.github.io/framian/latest/api#framian.Frame
[Cols]: https://tixxit.github.io/framian/latest/api#framian.Cols
[Rows]: https://tixxit.github.io/framian/latest/api#framian.Rows
[Rec]: https://tixxit.github.io/framian/latest/api#framian.Rec
[Cell]: https://tixxit.github.io/framian/latest/api#framian.Cell
[Value]: https://tixxit.github.io/framian/latest/api#framian.Value
[NA]: https://tixxit.github.io/framian/latest/api#framian.NA
[NM]: https://tixxit.github.io/framian/latest/api#framian.NM
[Series]: https://tixxit.github.io/framian/latest/api#framian.Series
