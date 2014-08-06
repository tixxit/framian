
organization in ThisBuild := "com.pellucid"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))


scalaVersion in ThisBuild := "2.11.2"

crossScalaVersions in ThisBuild := Seq("2.10.4", "2.11.2")

scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature", "-unchecked")


maxErrors in ThisBuild := 5


resolvers in ThisBuild ++= Seq(
  "Typesafe Repo"             at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.url("Side Typesafe Repo", url("http://repo.typesafe.com/typesafe/maven-ivy-releases"))(Resolver.ivyStylePatterns),
  "Sonatype Snapshots"        at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases"         at "http://oss.sonatype.org/content/repositories/releases"
)

lazy val root = project.
  in(file(".")).
  aggregate(framian, framianJsonBase, framianJsonPlay).
  settings(
    publish := (),
    publishLocal := ()
  )

lazy val framian = project.
  in(file("framian"))

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
