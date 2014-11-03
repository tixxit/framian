---
title: Framian Guide
---

Getting Started
===============

This tutorial is best if you follow along with me, trying the examples. To get
started, [clone the tutorial repo][Tutorial Repo], and start up a Scala REPL
from SBT.

```
$ git clone https://github.com/tixxit/framian-tutorial.git
$ cd framian-tutorial
$ sbt
> console
[info] Starting scala interpreter...
[info]
import framian._
import framian.csv.Csv
import framian.tutorial._
import spire.implicits._
import org.joda.time.LocalDate
Welcome to Scala version 2.11.2 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_45).
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Great! We'll just be using the REPL in this tutorial. Aside from brining in
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
`spire.math.Rational` or `spire.math.Number`. The `implicits` import just
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

```scala
scala> case class Company(name: String, exchange: String, ticker: String, currency: String, marketCap: BigDecimal)
defined class Company
```

This case class includes some basic information we may find useful later, such
as the company's reporting currency, their exchange, ticker, and a human
readable name. Let's create a few fake companies to populate a frame with.

```scala
scala> val Acme = Company("Acme", "NASDAQ", "ACME", "USD", BigDecimal("123.00"))
Acme: Company = Company(Acme,NASDAQ,ACME,USD,123.00)

scala> val BobCo = Company("Bob Company", "NASDAQ", "BOB", "USD", BigDecimal("45.67"))
Acme: Company = Company(Bob Company,NASDAQ,BOB,USD,45.67)

scala> val Cruddy = Company("Cruddy Inc", "XETRA", "CRUD", "EUR", BigDecimal("1.00"))
Cruddy: Company = Company(Cruddy Inc,XETRA,CRUD,EUR,1.00)
```

And now we create the frame using `Frame.fromRows`.

```scala
scala> Frame.fromRows(Acme, BobCo, Cruddy)
res0: framian.Frame[Int,Int] =
    0           . 1      . 2     . 3   . 4
0 : Acme        | NASDAQ | ACME  | USD | 123.00
1 : Bob Company | NASDAQ | BOB   | USD | 45.67
2 : Cruddy Inc  | XETRA  | CRUD  | EUR | 1.00
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

```scala
scala> res0.mapColKeys {
     |   case 0 => "Name"
     |   case 1 => "Exchange"
     |   case 2 => "Ticker"
     |   case 3 => "Currency"
     |   case 4 => "Market Cap"
     | }
res1: framian.Frame[Int,String] =
    Name        . Exchange . Ticker . Currency . Market Cap
0 : Acme        | NASDAQ   | ACME   | USD      | 123.00
1 : Bob Company | NASDAQ   | BOB    | USD      | 45.67
2 : Cruddy Inc  | XETRA    | CRUD   | EUR      | 1.00
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

```scala
scala> val ticker = Cols("Ticker").as[String]
ticker: framian.Cols[String,String] = Pick(List(Ticker),framian.RowExtractorLowPriorityImplicits$RowExtractorLow2$$anon$4@26874fe4)
```

When we construct a `Cols` instance using `Cols("Ticker")`, it will extract
each ticker symbol as a [`Rec`][Rec]. A `Rec` is rarely what we want, so we
use the `as[A]` method to tell the `Cols` instance to extract the column as
a type `A`.

Using `ticker`, we can now reindex our rows using the ticker symbol, instead
of the row index.

```scala
scala> val frame = res1.reindex(ticker)
        Name        . Exchange . Ticker . Currency . Market Cap
ACME  : Acme        | NASDAQ   | ACME   | USD      | 123.00
BOB   : Bob Company | NASDAQ   | BOB    | USD      | 45.67
CRUD  : Cruddy Inc  | XETRA    | CRUD   | EUR      | 1.00
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

```scala
scala> frame[BigDecimal]("ACME", "Market Cap")
res2: framian.Cell[BigDecimal] = Value(123.00)
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

```scala
scala> frame[Double]("BOB", "Market Cap")
res2: framian.Cell[Double] = Value(45.67)
```

You'll notice that for Acme, we asked for the cell as type `BigDecimal`, but
for Bob Co, we asked for the market cap as a `Double`. Both returned a value.
In Framian, we support conversions between most common numeric types found in
Scala and Spire. Framian attempts to keep numbers abstract, letting the user
decide what kind of precision/speed trade-off they want to make, rather than
forcing 1 type. Even though we had *stored* the data as a `BigDecimal`, when we
requested it as a `Double` Framian performed the conversion for us.

OK, well, what if we ask for a less sensible type?

```scala
scala> frame[LocalDate]("CRUD", "Market Cap")
res3: framian.Cell[LocalDate] = NM
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

```scala
scala> val marketCap = Cols("Market Cap").as[BigDecimal]
marketCap: framian.Cols[String,BigDecimal] = ...
```

We can use `marketCap` to extract a [`Series`][Series] from the frame.

```scala
scala> frame.get(marketCap)
res4: framian.Series[String,BigDecimal] = Series(ACME -> Value(123.00), BOB -> Value(45.67), XETRA -> Value(1.00))
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
frames. Let's fetch some real company info.

```scala
scala> fetchCompanyInfo("GM", "HMC", "BMW.DE")
res5: framian.Frame[Int,String] =
    Name              . Stock Exchange . Ticker . Currency . Market Cap
0 : General Motors Co | NYSE           | GM     | USD      | 50.097B
1 : Honda Motor Compa | NYSE           | HMC    | USD      | 58.394B
2 : BMW               | XETRA          | BMW.DE | EUR      | 55.959B
```

This looks very similar to our previous frame... almost like it was planned
that way. Well, let's get this reindexed by ticker.

```scala
scala> val companies = res5.reindex(ticker)
companies: framian.Frame[String,String] =
         Name              . Stock Exchange . Ticker . Currency . Market Cap
GM     : General Motors Co | NYSE           | GM     | USD      | 50.097B
HMC    : Honda Motor Compa | NYSE           | HMC    | USD      | 58.394B
BMW.DE : BMW               | XETRA          | BMW.DE | EUR      | 55.959B
```

[Tutorial Repo]: https://github.com/tixxit/framian-tutorial/
[Spire]: https://github.com/non/spire
[Frame]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Frame
[Cols]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Cols
[Rows]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Rows
[Rec]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Rec
[Cell]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Cell
[Value]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Value
[NA]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.NA
[NM]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.NM
[Series]: https://pellucidanalytics.github.io/framian/api/current/index.html#framian.Series
