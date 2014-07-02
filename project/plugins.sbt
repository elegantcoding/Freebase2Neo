resolvers ++= Seq(
  "less is" at "http://repo.lessis.me",
  "Sonatype" at "http://oss.sonatype.org/content/repositories/releases/",
  Classpaths.sbtPluginReleases)

addSbtPlugin("me.lessis" % "ls-sbt" % "0.1.2")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.5.1")

addSbtPlugin("org.scoverage" %% "sbt-coveralls" % "0.98.0")

