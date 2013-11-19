package pellucid.pframe

import shapeless.Nat

sealed trait Size

final class Fixed[N <: Nat] extends Size
final object Fixed {
  implicit def fixed[N <: Nat]: Fixed[N] = new Fixed[N]
}

final class Variable extends Size
final object Variable {
  implicit val variable = new Variable
}

