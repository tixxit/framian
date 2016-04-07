name := "framian"

(sourceGenerators in Compile) <+= (sourceManaged in Compile) map Boilerplate.gen

libraryDependencies ++= {
  import Dependencies._
  Seq(
    Compile.spire,
    Compile.shapeless,
    Test.discipline,
    Test.specs2,
    Test.scalaCheck,
    Test.spireLaws
  )
}

initialCommands := """
| import framian._
| import shapeless._
| import spire.implicits._""".stripMargin('|')


testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "junitxml", "console")

Publish.settings

// Scaladoc publishing

enablePlugins(SiteScaladocPlugin)

ghpages.settings

git.remoteRepo := "git@github.com:tixxit/framian.git"
