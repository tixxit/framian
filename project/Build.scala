import sbt._
import Keys._


object BuildSettings {

  val buildOrganization = "com.pellucid"
  val buildVersion      = "0.1.1"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization                  := buildOrganization,
    version                       := buildVersion,
    scalaVersion                  := "2.11.1",
    crossScalaVersions            := Seq("2.10.4", "2.11.1"),
    maxErrors                     := 5,
    scalacOptions                ++= Seq("-deprecation", "-feature", "-unchecked", "-language:higherKinds")
  )

}


object build extends Build {


  val repositories = Seq(
    "Typesafe Repo"             at "http://repo.typesafe.com/typesafe/releases/",
    Resolver.url("Side Typesafe Repo", url("http://repo.typesafe.com/typesafe/maven-ivy-releases"))(Resolver.ivyStylePatterns),
    "Sonatype Snapshots"        at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases"         at "http://oss.sonatype.org/content/repositories/releases"
  )

  lazy val sharedSettings =
    BuildSettings.buildSettings ++ bintray.Plugin.bintrayPublishSettings ++ Seq(
      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
      bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("pellucid")
    )

  // CORE PROJECT
  lazy val root = Project(
    id = "root",
    base = file("."),
    aggregate = Seq(framian, framianJsonBase, framianJsonPlay),
    settings = sharedSettings ++ Seq(publish := (), publishLocal := ())
  )

  def framianSettings =
    sharedSettings ++
    Seq(
      testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "html", "junitxml", "console"),
      resolvers ++= repositories,
      initialCommands := """
      | import framian._
      | import shapeless._
      | import spire.implicits._""".stripMargin('|'),
      libraryDependencies ++= Dependencies.framian,
      libraryDependencies += (
        if (isScala11orLater(scalaVersion.value)) Dependency.shapeless_11 else Dependency.shapeless_10
      )
    )

  def isScala11orLater(versionString: String): Boolean =
    CrossVersion.scalaApiVersion(versionString) match {
        case Some((2, x)) if x >= 11 => true
        case _ => false
      }

  lazy val framian = Project(
    id = "framian",
    base = file("framian"),
    settings = framianSettings
  )

  lazy val framianJsonBase = Project(
    id = "framian-json-base",
    base = file("framian-json-base"),
    dependencies = Seq(framian),
    settings = framianSettings
  )

  lazy val framianJsonPlay = Project(
    id = "framian-json-play",
    base = file("framian-json-play"),
    dependencies = Seq(framianJsonBase),
    settings = framianSettings ++ Seq(
      libraryDependencies += Dependency.playJson_23
    )
  )

  lazy val framianJsonPlay22 = Project(
    id = "framian-json-play22",
    base = file("framian-json-play22"),
    dependencies = Seq(framianJsonBase),
    settings = framianSettings ++ Seq(
      name := "framian-json-play22",
      scalaVersion := "2.10.4",
      crossScalaVersions := Seq("2.10.4"),
      libraryDependencies += Dependency.playJson_22,
      sourceDirectory <<= sourceDirectory in framianJsonPlay
    )
  )
}


object Dependencies {
  import Dependency._
  val framian = Seq(spire, jodaTime, jodaConvert, Test.discipline, Test.specs2, Test.scalaCheck, Test.spireLaws)
}

object Dependency {

  object V {
    val Play_22            = "2.2.3"
    val Play_23            = "2.3.1"

    val Spire              = "0.7.4"
    val Shapeless          = "2.0.0"
    val Discipline         = "0.2.1"

    val JodaTime           = "2.3"
    val JodaConvert        = "1.6"

    // Test libraries
    val Specs2             = "2.3.12"
    val ScalaCheck         = "1.11.4"
  }

  // Compile
  val playJson_22        =   "com.typesafe.play"                    %% "play-json"               % V.Play_22
  val playJson_23        =   "com.typesafe.play"                    %% "play-json"               % V.Play_23

  val spire              =   "org.spire-math"                       %% "spire"                   % V.Spire
  val shapeless_10       =   "com.chuusai"                           % "shapeless_2.10.4"        % V.Shapeless
  val shapeless_11       =   "com.chuusai"                           % "shapeless_2.11"          % V.Shapeless

  val jodaTime           =  "joda-time"                              % "joda-time"               % V.JodaTime
  val jodaConvert        =  "org.joda"                               % "joda-convert"            % V.JodaConvert

  // Test
  object Test {
    val specs2           =   "org.specs2"                           %% "specs2"                  % V.Specs2        % "test"
    val scalaCheck       =   "org.scalacheck"                       %% "scalacheck"              % V.ScalaCheck    % "test"
    val spireLaws        =   "org.spire-math"                       %% "spire-scalacheck-binding"% V.Spire         % "test"
    val discipline       =   "org.typelevel"                        %% "discipline"              % V.Discipline    % "test"
  }
}

