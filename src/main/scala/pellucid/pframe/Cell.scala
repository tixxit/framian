package pellucid
package pframe

sealed trait Cell[+A] {
  def isMissing: Boolean
  def value: Option[A]
}

sealed trait Missing extends Cell[Nothing] {
  def isMissing = true
  def value = None
}

case object NA extends Missing
case object NM extends Missing

case class Value[+A](get: A) extends Cell[A] {
  def value = Some(get)
  def isMissing = false
}
