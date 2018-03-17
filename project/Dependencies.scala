import sbt._

object Dependencies {

  object V {
    val Spire              = "0.13.0"
    val Shapeless          = "2.3.3"
    val Discipline         = "0.9.0"

    // Test libraries
    val ScalaTest          = "3.0.5"
    val ScalaCheck         = "1.12.5"
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
    val spireLaws       =   "org.spire-math"        %% "spire-laws"                  % V.Spire         % "test"
    val discipline      =   "org.typelevel"         %% "discipline"                  % V.Discipline    % "test"
  }
}
