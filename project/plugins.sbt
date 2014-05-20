// Comment to get more information during initialization
logLevel := Level.Info

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

// The Typesafe repository
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Pellucid Artifactory" at "http://pellucid.artifactoryonline.com/pellucid/repo"

resolvers += Resolver.url(
  "sbt-plugin-releases",
  url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/")
)(Resolver.ivyStylePatterns)

resolvers += "spray repo" at "http://repo.spray.io"


addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.1")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.2.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("org.ensime" % "ensime-sbt-cmd" % "0.1.2")

addSbtPlugin("fi.gekkio.sbtplugins" %% "sbt-jrebel-plugin" % "0.10.0")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.1")
