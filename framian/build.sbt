
name := "framian"

(sourceGenerators in Compile) <+= (sourceManaged in Compile) map Boilerplate.gen

libraryDependencies ++= {
  import Dependencies._
  Seq(
    Compile.spire,
    Test.discipline,
    Test.specs2,
    Test.scalaCheck,
    Test.spireLaws
  )
}

libraryDependencies += (
  CrossVersion.scalaApiVersion(scalaVersion.value) match {
    case Some((2, x)) if x >= 11 => Dependencies.Compile.shapeless_11
    case _ => Dependencies.Compile.shapeless_10
  }
)

initialCommands := """
| import framian._
| import shapeless._
| import spire.implicits._""".stripMargin('|')

testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "junitxml", "console")

TestCoverage.settings

Publish.settings
