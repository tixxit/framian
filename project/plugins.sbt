
// https://issues.scala-lang.org/browse/SI-8772
// trick from https://github.com/sbt/sbt/issues/1439#issuecomment-51860493
def customAddSbtPlugin(m: ModuleID) = Defaults.sbtPluginExtra(m, "0.13", "2.10") excludeAll ExclusionRule("org.scala-lang")

libraryDependencies ++= Seq(
  customAddSbtPlugin("de.johoop" % "jacoco4sbt" % "2.1.6"),
  customAddSbtPlugin("me.lessis" % "bintray-sbt" % "0.1.2")
)
