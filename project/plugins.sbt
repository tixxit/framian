resolvers += Resolver.url("tixxit/sbt-plugins", url("http://dl.bintray.com/tixxit/sbt-plugins"))(Resolver.ivyStylePatterns)
resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.jsuereth"       % "sbt-pgp"       % "1.0.0")
addSbtPlugin("com.github.gseitz"  % "sbt-release"   % "1.0.0")
addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"  % "0.5.0")
addSbtPlugin("com.typesafe.sbt"   % "sbt-site"      % "1.0.0")
addSbtPlugin("com.typesafe.sbt"   % "sbt-ghpages"   % "0.5.4")
addSbtPlugin("org.scoverage"      % "sbt-scoverage" % "1.1.0")
addSbtPlugin("net.tixxit"         % "sbt-benchmark" % "0.1.1")
