import sbtassembly.Plugin.{MergeStrategy, AssemblyKeys}
import AssemblyKeys._
import ls.Plugin.LsKeys

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList(ps @ _*) if ps.last endsWith "CHANGES.txt" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "LICENSES.txt" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "ComponentVersion.class" => MergeStrategy.last
  case x => old(x)
}
}

test in assembly := {}

name := "freebase2neo"

version := "0.2.0"

organization := "org.anormcypher"

scalaVersion := "2.11.1"

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-feature")

resolvers += "Elegant Coding Releases" at "http://elegantcoding.github.io/repo/releases"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "commons-io" % "commons-io" % "2.4" % "test",
  "com.typesafe" % "config" % "1.0.2",
  "org.neo4j" % "neo4j" % "2.1.2",
  "org.clapper" %% "grizzled-slf4j" % "1.0.2",
  "com.googlecode.lanterna" % "lanterna" % "2.1.8",
  "com.elegantcoding" %% "rdf-processor" % "0.5.0"
)

fork in Test := true

javaOptions in Test += "-Xmx5g"

seq(lsSettings :_*)

(LsKeys.tags in LsKeys.lsync) := Seq("rdf","neo4j", "neo")

(description in LsKeys.lsync) :=
  "Convert RDF to neo4j."

instrumentSettings

coverallsSettings

CoverallsKeys.coverallsToken := Some("QlYKhxhAQsUyt8pYa2XsOKHOdcJGun7TE")
