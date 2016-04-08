import sbt._

object Dependencies {

  object V {
    val Spire              = "0.8.2"
    val Shapeless          = "2.3.0"
    val Discipline         = "0.2.1"

    // Test libraries
    val ScalaTest          = "2.2.6"
    val ScalaCheck         = "1.11.6"
  }

  // Compile
  object Compile {
    val spire             = "org.spire-math"           %% "spire"                   % V.Spire
    val shapeless         = "com.chuusai"              %% "shapeless"               % V.Shapeless
  }

  // Test
  object Test {
    val scalaTest       =   "org.scalatest"         %% "scalatest"                   % V.ScalaTest     % "test"
    val scalaCheck      =   "org.scalacheck"        %% "scalacheck"                  % V.ScalaCheck    % "test"
    val spireLaws       =   "org.spire-math"        %% "spire-scalacheck-binding"    % V.Spire         % "test"
    val discipline      =   "org.typelevel"         %% "discipline"                  % V.Discipline    % "test"
  }
}
