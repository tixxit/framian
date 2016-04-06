organization in ThisBuild := "com.pellucid"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion in ThisBuild := "2.11.8"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:higherKinds",
  "-optimize")

maxErrors in ThisBuild := 5

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases")
)

lazy val root = project.
  in(file(".")).
  aggregate(framianMacros, framian, framianBenchmarks).
  settings(
    publish := (),
    publishLocal := ()
  )

lazy val framianMacros = project.
  in(file("framian-macros"))

lazy val framian = project.
  in(file("framian")).
  dependsOn(framianMacros).
  settings(
    // map framian-macros project classes and sources into framian
    mappings in (Compile, packageBin) <++= mappings in (framianMacros, Compile, packageBin),
    mappings in (Compile, packageSrc) <++= mappings in (framianMacros, Compile, packageSrc),
    Publish.pomDependencyExclusions := Seq("com.pellucid" -> s"framian-macros_${scalaBinaryVersion.value}")
  )

lazy val framianBenchmarks = project.
  in(file("framian-benchmarks")).
  enablePlugins(BenchmarkPlugin).
  dependsOn(framian)
