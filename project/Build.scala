import sbt._
import Keys._


object BuildSettings {

  val buildOrganization = "pellucid.content"
  val buildVersion      = "0.0.1"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization                  := buildOrganization,
    version                       := buildVersion,
    scalaVersion                  := "2.11.1",
    crossScalaVersions            := Seq("2.10.4", "2.11.1"),
    maxErrors                     := 5,
    scalacOptions                ++= Seq("-deprecation", "-feature", "-unchecked")
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
    BuildSettings.buildSettings

  // CORE PROJECT
  lazy val root = Project(
    id = "root",
    base = file("."),
    aggregate = Seq(pframe, pframeJsonBase, pframeJsonPlay),
    settings = sharedSettings
  )

  def pframeSettings =
    sharedSettings ++
    Seq(
      testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "html", "junitxml", "console"),
      resolvers ++= repositories,
      initialCommands := """
      | import pellucid.pframe._
      | import shapeless._
      | import spire.implicits._""".stripMargin('|'),
      libraryDependencies ++= Dependencies.pframe,
      libraryDependencies += (
        if (isScala11orLater(scalaVersion.value)) Dependency.shapeless_11 else Dependency.shapeless_10
      )
    )

  def isScala11orLater(versionString: String): Boolean =
    CrossVersion.scalaApiVersion(versionString) match {
        case Some((2, x)) if x >= 11 => true
        case _ => false
      }

  lazy val pframe = Project(
    id = "pframe",
    base = file("modules/pframe"),
    settings = pframeSettings
  )

  lazy val pframeJsonBase = Project(
    id = "pframe-json-base",
    base = file("modules/pframe-json-base"),
    dependencies = Seq(pframe),
    settings = pframeSettings
  )

  lazy val pframeJsonPlay = Project(
    id = "pframe-json-play",
    base = file("modules/pframe-json-play"),
    dependencies = Seq(pframeJsonBase),
    settings = pframeSettings ++ Seq(
      libraryDependencies += (
        if (isScala11orLater(scalaVersion.value)) Dependency.playJson_23 else Dependency.playJson_22
      )
    )
  )

}


object Dependencies {
  import Dependency._
  val pframe = Seq(spire, jodaTime, jodaConvert, Test.discipline, Test.specs2, Test.scalaCheck, Test.spireLaws)
}

object Dependency {

  object V {
    val Play_22            = "2.2.3"
    val Play_23            = "2.3.0"

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

