package framian

import org.scalactic.TypeCheckedTripleEquals

import org.scalatest.{ Matchers, WordSpec }

trait FramianSpec extends WordSpec
  with Matchers
  with TypeCheckedTripleEquals

