package framian
package column

import scala.{specialized => sp }

private[framian] sealed trait DenseColumn[@sp(Int,Long,Double) A] extends UnboxedColumn[A] {
  def values: Array[_]
  def naValues: Mask
  def nmValues: Mask

  private final def valid(row: Int) = row >= 0 && row < values.length
  def isValueAt(row: Int): Boolean = valid(row) && !naValues(row) && !nmValues(row)
  def nonValueAt(row: Int): NonValue = if (nmValues(row)) NM else NA

  def filter(p: A => Boolean): Column[A] = {
    val na = Mask.newBuilder
    var i = 0
    while (i < values.length) {
      if (naValues(i) || (isValueAt(i) && !p(valueAt(i)))) {
        na += i
      }
      i += 1
    }
    Column.dense(values, na.result(), nmValues).asInstanceOf[Column[A]]
  }

  def mask(na: Mask): Column[A] =
    Column.dense(values, naValues | na, nmValues -- na).asInstanceOf[Column[A]]

  def setNA(row: Int): Column[A] =
    if ((row < 0 || row >= values.length) && !nmValues(row)) this
    else Column.dense(values, naValues + row, nmValues - row).asInstanceOf[Column[A]]

  def cellMap[B](f: Cell[A] => Cell[B]): Column[B] = Column.eval(apply _).cellMap(f)

  def memoize(optimistic: Boolean): Column[A] = this

  def flatMap[B](f: A => Cell[B]): Column[B] = {
    val bldr = Column.newBuilder[B]()
    var i = 0
    while (i < values.length) {
      if (nmValues(i)) {
        bldr.addNM()
      } else if (naValues(i)) {
        bldr.addNA()
      } else {
        bldr.add(f(valueAt(i)))
      }
      i += 1
    }
    bldr.result()
  }

  def shift(n: Int): Column[A] = {
    if (n < 0) {
      Column.eval(apply _).shift(n)
    } else {
      val len = spire.math.min(values.length.toLong + n, Int.MaxValue.toLong).toInt
      val indices = Array.fill(len)(-1)
      var i = n
      while (i < len) {
        indices(i) = i - n
        i += 1
      }
      reindex(indices)
    }
  }

  override def toString: String = {
    val len = nmValues.max.map(_ + 1).getOrElse(values.length)
    (0 until len).map(apply(_).toString).mkString("Column(", ", ", ")")
  }
}

private[framian] object DenseColumn extends DenseColumnFunctions

private[framian] case class IntColumn(values: Array[Int], naValues: Mask, nmValues: Mask) extends DenseColumn[Int] {
  def valueAt(row: Int): Int = values(row)
  def map[@sp(Int,Long,Double) B](f: Int => B): Column[B] = DenseColumn.mapInt(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Int] = DenseColumn.reindexInt(index, values, naValues, nmValues)
  def force(len: Int): Column[Int] = {
    if (values.length <= len) {
      val nm = if (nmValues.max.getOrElse(-1) < len) nmValues
               else nmValues.filter(_ < len)
      IntColumn(values, naValues, nm)
    } else {
      IntColumn(
        java.util.Arrays.copyOf(values, len),
        if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
        nmValues.filter(_ < len)
      )
    }
  }
  def orElse[A0 >: Int](that: Column[A0]): Column[A0] = DenseColumn.orElseInt(values, naValues, nmValues, that)
}

private[framian] case class LongColumn(values: Array[Long], naValues: Mask, nmValues: Mask) extends DenseColumn[Long] {
  def valueAt(row: Int): Long = values(row)
  def map[@sp(Int,Long,Double) B](f: Long => B): Column[B] = DenseColumn.mapLong(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Long] = DenseColumn.reindexLong(index, values, naValues, nmValues)
  def force(len: Int): Column[Long] = {
    if (values.length <= len) {
      val nm = if (nmValues.max.getOrElse(-1) < len) nmValues
               else nmValues.filter(_ < len)
      LongColumn(values, naValues, nm)
    } else {
      LongColumn(
        java.util.Arrays.copyOf(values, len),
        if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
        nmValues.filter(_ < len)
      )
    }
  }
  def orElse[A0 >: Long](that: Column[A0]): Column[A0] = DenseColumn.orElseLong(values, naValues, nmValues, that)
}

private[framian] case class DoubleColumn(values: Array[Double], naValues: Mask, nmValues: Mask) extends DenseColumn[Double] {
  def valueAt(row: Int): Double = values(row)
  def map[@sp(Int,Long,Double) B](f: Double => B): Column[B] = DenseColumn.mapDouble(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[Double] = DenseColumn.reindexDouble(index, values, naValues, nmValues)
  def force(len: Int): Column[Double] = {
    if (values.length <= len) {
      val nm = if (nmValues.max.getOrElse(-1) < len) nmValues
               else nmValues.filter(_ < len)
      DoubleColumn(values, naValues, nm)
    } else {
      DoubleColumn(
        java.util.Arrays.copyOf(values, len),
        if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
        nmValues.filter(_ < len)
      )
    }
  }
  def orElse[A0 >: Double](that: Column[A0]): Column[A0] = DenseColumn.orElseDouble(values, naValues, nmValues, that)
}

private[framian] case class AnyColumn[A](values: Array[Any], naValues: Mask, nmValues: Mask) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row).asInstanceOf[A]
  def map[@sp(Int,Long,Double) B](f: A => B): Column[B] = DenseColumn.mapAny(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[A] = DenseColumn.reindexAny(index, values, naValues, nmValues)
  def force(len: Int): Column[A] = {
    if (values.length <= len) {
      val nm = if (nmValues.max.getOrElse(-1) < len) nmValues
               else nmValues.filter(_ < len)
      AnyColumn(values, naValues, nm)
    } else {
      AnyColumn(
        DenseColumn.copyArray(values, len),
        if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
        nmValues.filter(_ < len)
      )
    }
  }
  def orElse[A0 >: A](that: Column[A0]): Column[A0] = DenseColumn.orElseAny[A, A0](values, naValues, nmValues, that)
}

private[framian] case class GenericColumn[A](values: Array[A], naValues: Mask, nmValues: Mask) extends DenseColumn[A] {
  def valueAt(row: Int): A = values(row)
  def map[@sp(Int,Long,Double) B](f: A => B): Column[B] = DenseColumn.mapGeneric(values, naValues, nmValues, f)
  def reindex(index: Array[Int]): Column[A] = DenseColumn.reindexGeneric(index, values, naValues, nmValues)
  def force(len: Int): Column[A] = {
    if (values.length <= len) {
      val nm = if (nmValues.max.getOrElse(-1) < len) nmValues
               else nmValues.filter(_ < len)
      GenericColumn(values, naValues, nm)
    } else {
      GenericColumn(
        DenseColumn.copyArray(values, len),
        if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
        nmValues.filter(_ < len)
      )
    }
  }
  def orElse[A0 >: A](that: Column[A0]): Column[A0] = DenseColumn.orElseGeneric[A, A0](values, naValues, nmValues, that)
}

