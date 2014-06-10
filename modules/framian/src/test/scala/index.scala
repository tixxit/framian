import org.specs2._
import runner.SpecificationsFinder._

class index extends Specification { def is =

  framianSpecs("Framian Specifications")

  // see the SpecificationsFinder trait for the parameters of the 'specifications' method
  def framianSpecs(t: String) = t.title ^ specifications(basePath = "modules/framian/src/test/scala").map(see)
}
