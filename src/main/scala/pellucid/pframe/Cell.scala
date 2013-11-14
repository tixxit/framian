package pellucid
package pframe

sealed trait Cell[+A]
sealed trait Missing extends Cell[Nothing]
case object NA extends Missing
case object NM extends Missing
case class Value[+A](value: A) extends Cell[A]
