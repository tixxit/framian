package framian.stats

import scala.reflect.ClassTag

import spire.algebra._
import spire.implicits._

case class LinearRegression[A: ClassTag](beta: Array[A], alpha: A) {
  def apply(p: Array[A])(implicit A: Field[A]): A = (p dot beta) + alpha

  def map[B: ClassTag](f: A => B): LinearRegression[B] =
    LinearRegression(beta map f, f(alpha))
}

