import org.specs2._
import runner.SpecificationsFinder._

class index extends Specification { def is =

  pframeSpecs("pframe specifications")

  // see the SpecificationsFinder trait for the parameters of the 'specifications' method
  def pframeSpecs(t: String) = t.title ^ specifications(basePath = "modules/pframe/src/test/scala").map(see)
}
