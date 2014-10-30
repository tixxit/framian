
name := "framian-stats"

libraryDependencies ++= {
  import Dependencies._
  Seq(
    Compile.spire,
    Compile.commonsMath3,
    Test.discipline,
    Test.specs2,
    Test.scalaCheck
  )
}

testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "html", "junitxml", "console")

TestCoverage.settings