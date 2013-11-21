package pellucid
package pframe

trait ColumnLike[+This] {
  def mask(bits: Int => Boolean): This
  def shift(rows: Int): This
  def reindex(index: Array[Int]): This
  def setNA(row: Int): This
}
