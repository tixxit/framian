
name := "framian-column"

libraryDependencies ++= {
  import Dependencies._
  Seq(
    Compile.spire,
    Test.discipline,
    Test.specs2,
    Test.scalaCheck
  )
}

unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / s"scala_${scalaBinaryVersion.value}"

initialCommands := """
  |import framian._
""".stripMargin('|')


testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "html", "junitxml", "console")

TestCoverage.settings

Publish.settings

Dependencies.macroParadise
