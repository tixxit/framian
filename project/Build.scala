import sbt._
import Keys._
import org.apache.ivy.core.module.id.ModuleRevisionId
import org.apache.ivy.core.install.InstallOptions
import scala.Some
import fi.gekkio.sbtplugins.jrebel.JRebelPlugin
import spray.revolver.RevolverPlugin._
import play.Project._

object BuildSettings {

  val buildOrganization = "pellucid.content"
  val buildScalaVersion = "2.10.3"
  val buildVersion      = "0.0.1"

  val release           = settingKey[Boolean]("Perform release")
  val gitHeadCommitSha  = settingKey[String]("current git commit SHA")

  def getPublishTo(release: Boolean) = {
    val artifactory = "http://pellucid.artifactoryonline.com/pellucid/"
    if (!release)
      Some("lib-snapshots-local" at artifactory + "libs-snapshots-local")
    else
      Some("lib-releases-local" at artifactory + "libs-releases-local")
  }

  def getVersion(release: Boolean, sha: String) = {
    import java.util.{Calendar, TimeZone}
    import java.text.SimpleDateFormat
    val utcTz = TimeZone.getTimeZone("UTC")
    val cal = Calendar.getInstance(utcTz)
    val sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    sdf.setCalendar(cal)
    if (!release) s"$buildVersion-${sdf.format(cal.getTime)}-$sha" else buildVersion
  }

  val buildSettings = Seq(
    organization                  := buildOrganization,
    scalaVersion                  := buildScalaVersion,
    shellPrompt                   := ShellPrompt.buildShellPrompt,
    maxErrors                     := 5,
    scalacOptions                ++= Seq("-deprecation"), //, "-Xlog-implicits"),
    credentials                   += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    release                       := sys.props("data-api-release") == "true",
    gitHeadCommitSha in ThisBuild := Process("git rev-parse --short HEAD").lines.head,
    version in ThisBuild          := getVersion(release.value, gitHeadCommitSha.value),
    publishTo                     := getPublishTo(release.value),
    publishMavenStyle             := true
  )

  val localRepo = Seq(
    Resolver.file("Local Play IVY2 Repository", file(Path.userHome.absolutePath+"/.ivy2/local"))(Resolver.ivyStylePatterns)
  )
}


object ApplicationBuild extends Build {

  seq(JRebelPlugin.jrebelSettings: _*)
  seq(Revolver.settings: _*)

  implicit def toRichProject(project: Project) = new RichProject(project)

  val moreResolvers = resolvers ++= Seq(
    "Pellucid Artifactory" at "http://pellucid.artifactoryonline.com/pellucid/repo",

    "Typesafe Repo"             at "http://repo.typesafe.com/typesafe/releases/",
    Resolver.url("Side Typesafe Repo", url("http://repo.typesafe.com/typesafe/maven-ivy-releases"))(Resolver.ivyStylePatterns),
    "Local Maven Repository"    at "file://"+Path.userHome.absolutePath+"/.m2/repository", // Used for local install of Datomic
    "Sonatype Snapshots"        at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases"         at "http://oss.sonatype.org/content/repositories/releases",

    "JBoss"                     at "https://repository.jboss.org/nexus/content/groups/public",
    "Expecty Repository"        at "https://raw.github.com/pniederw/expecty/master/m2repo/"
  ) ++ BuildSettings.localRepo

  // CORE PROJECT
  lazy val root = Project(
    id = "root",
    base = file(".")
  ) aggregate (pframe, pframeJsonBase, pframeJsonPlay)

  def pframeSettings =
    Defaults.defaultSettings ++ BuildSettings.buildSettings ++ Seq(
      shellPrompt := ShellPrompt.buildShellPrompt,
      moreResolvers,
      initialCommands := """
      | import pellucid.pframe._
      | import shapeless._
      | import spire.implicits._""".stripMargin('|'),
      libraryDependencies ++= Dependencies.pframe
    ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings

  lazy val pframe = Project("pframe", file("modules/pframe")).
    settings(pframeSettings: _*)

  lazy val pframeJsonBase = Project("pframe-json-base", file("modules/pframe-json-base")).
    settings(pframeSettings: _*).
    dependsOn(pframe)

  lazy val pframeJsonPlay = Project("pframe-json-play", file("modules/pframe-json-play")).
    settings(pframeSettings: _*).
    settings(libraryDependencies += Dependency.playJson).
    dependsOn(pframe, pframeJsonBase)
}

class RichProject(project: Project)  {
  def projectDefaults = project.settings(
    resourceDirectories in Test <++= resourceDirectories in Compile
  )
}

object ShellPrompt {
  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }

  val current = """\*\s+([\w-]+)""".r

  def gitBranches = ("git branch --no-color" lines_! devnull mkString)

  val buildShellPrompt = {
    (state: State) => {
      val currBranch =
        current findFirstMatchIn gitBranches map (_ group(1)) getOrElse "-"
      val currProject = Project.extract (state).currentProject.id
      "%s:%s> ".format (
        currProject, currBranch
      )
    }
  }
}

object Dependencies {
  import Dependency._
  val pframe = Seq(shapeless, spire, jodaTime, jodaConvert, Test.discipline, Test.specs2, Test.scalaCheck, Test.spireLaws)
}

object Dependency {

  object V {
    val Play               = play.core.PlayVersion.current

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
  val playJson           =   "com.typesafe.play"                    %% "play-json"               % V.Play

  val spire              =   "org.spire-math"                       %% "spire"                   % V.Spire
  val shapeless          =   "com.chuusai"                           % "shapeless_2.10.2"        % V.Shapeless

  val jodaTime           =  "joda-time"                              % "joda-time"               % V.JodaTime
  val jodaConvert        =  "org.joda"                               % "joda-convert"            % V.JodaConvert

  // Test
  object Test {
    val specs2           =   "org.specs2"                           %% "specs2"                  % V.Specs2        % "test"
    val scalaCheck       =   "org.scalacheck"                       %% "scalacheck"              % V.ScalaCheck    % "test"
    val spireLaws        =   "org.spire-math"                       %% "spire-scalacheck-binding"% V.Spire
    val discipline       =   "org.typelevel"                        %% "discipline"              % V.Discipline
  }
}

