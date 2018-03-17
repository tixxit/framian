organization in ThisBuild := "net.tixxit"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion in Global := "2.12.4"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:higherKinds")

maxErrors in ThisBuild := 5

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases")
)

addCommandAlias("publishDocs", ";framian/packageDoc;framian/ghpagesPushSite")

lazy val root = project.
  in(file(".")).
  aggregate(framianMacros, framian, framianBenchmarks, docs).
  settings(Publish.skip: _*)

lazy val framianMacros = project.
  in(file("framian-macros")).
  settings(Publish.skip: _*)

lazy val framian = project.
  in(file("framian")).
  dependsOn(framianMacros).
  settings(
    // map framian-macros project classes and sources into framian
    mappings in (Compile, packageBin) <++= mappings in (framianMacros, Compile, packageBin),
    mappings in (Compile, packageSrc) <++= mappings in (framianMacros, Compile, packageSrc),
    Publish.pomDependencyExclusions := Seq("net.tixxit" -> s"framian-macros_${scalaBinaryVersion.value}")
  )

lazy val framianBenchmarks = project.
  in(file("framian-benchmarks")).
  enablePlugins(BenchmarkPlugin).
  dependsOn(framian).
  settings(Publish.skip: _*)

lazy val docs = project.
  in(file("docs")).
  dependsOn(framian).
  settings(Publish.skip: _*).
  enablePlugins(TutPlugin)