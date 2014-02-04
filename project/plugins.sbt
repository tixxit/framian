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

libraryDependencies ++= Seq(
  "com.github.spullara.mustache.java" % "compiler"      % "0.8.6",
  "com.amazonaws"                     % "aws-java-sdk"  % "1.3.33"
                                                          exclude("org.slf4j", "slf4j-nop")
                                                          exclude("org.jboss.netty", "netty")
                                                          exclude("net.java.dev.jets3t", "jets3t")
                                                          exclude("org.apache.httpcomponents", "httpclient"),
  "org.joda"                          % "joda-convert"  % "1.2",
  "joda-time"                         % "joda-time"     % "2.1",
  "commons-lang"                      % "commons-lang"  % "2.6"
)

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.1")

///addSbtPlugin("com.gu" % "sbt-grunt-plugin" % "0.1")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.2.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("com.inplaytime" %% "sbt-grunt-task" % "0.2")

//addSbtPlugin("com.untyped" %% "sbt-js" % "0.5")

addSbtPlugin("org.ensime" % "ensime-sbt-cmd" % "0.1.2")

addSbtPlugin("fi.gekkio.sbtplugins" %% "sbt-jrebel-plugin" % "0.10.0")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.1")
