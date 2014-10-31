
organization in ThisBuild := "com.pellucid"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))


scalaVersion in ThisBuild := "2.11.2"

crossScalaVersions in ThisBuild := Seq("2.10.4", "2.11.2")

scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature", "-unchecked", "-language:higherKinds", "-optimize")


maxErrors in ThisBuild := 5


resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases")
)

lazy val root = project.
  in(file(".")).
  aggregate(framianMacros, framian, framianJsonBase, framianJsonPlay, framianBenchmarks).
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

lazy val framianJsonBase = project.
  in(file("framian-json-base")).
  dependsOn(framian)

lazy val framianJsonPlay = project.
  in(file("framian-json-play")).
  dependsOn(framianJsonBase)

lazy val framianJsonPlay22 = project.
  in(file("framian-json-play22")).
  dependsOn(framianJsonBase).
  settings(
    sourceDirectory <<= sourceDirectory in framianJsonPlay
  )

lazy val framianBenchmarks = project.
  in(file("framian-benchmarks")).
  enablePlugins(BenchmarkPlugin).
  dependsOn(framian)
