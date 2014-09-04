
import net.tixxit.sbt.benchmark.BenchmarkPlugin._

name := "framian-column"

(sourceGenerators in Compile) <+= (sourceManaged in Compile) map Boilerplate.gen

libraryDependencies ++= {
  import Dependencies._
  Seq(
    Compile.spire,
    Test.discipline,
    Test.specs2,
    Test.scalaCheck,
    Benchmark.jmh
  )
}

initialCommands := """
  |import framian._
""".stripMargin('|')


testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "html", "junitxml", "console")

benchmark.settings

TestCoverage.settings

Publish.settings
