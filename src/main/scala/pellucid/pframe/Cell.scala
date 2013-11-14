package pellucid
package pframe

sealed trait Cell[+A] {
  def isMissing: Boolean
}

sealed trait Missing extends Cell[Nothing] {
  def isMissing = true
}

case object NA extends Missing
case object NM extends Missing

case class Value[+A](value: A) extends Cell[A] {
  def isMissing = false
}
