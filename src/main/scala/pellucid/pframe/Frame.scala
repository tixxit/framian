package pellucid
package pframe

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import spire.algebra._
import shapeless._

case class Frame[Row,Col](rowIndex: Index[Row], colIndex: Index[Col], cols: Array[Column[_]]) {
  def rawColumn[A: Typeable: TypeTag](k: Col): Option[Column[A]] = for {
    i <- colIndex.get(k)
  } yield Column.cast[A](cols(i))

  def column[A: Typeable: TypeTag](k: Col): Option[Series[Row,A]] =
    rawColumn(k) map (Series(rowIndex, _))

  def row[A](k: Row)(implicit t: Typeable[Column[A]]): Option[Series[Col,A]] = ???
}

object Frame {
  def apply[Row,Col: Order: ClassTag](rowIndex: Index[Row], colPairs: (Col,Column[_])*): Frame[Row,Col] = {
    val (colKeys, cols) = colPairs.unzip
    Frame(rowIndex, Index(colKeys: _*), cols.toArray)
  }
}
