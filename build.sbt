import sbtassembly.Plugin.AssemblyKeys
import AssemblyKeys._
import ls.Plugin.LsKeys

assemblySettings

name := "freebase2neo"

version := "0.2.0"

organization := "org.anormcypher"

scalaVersion := "2.11.1"

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-feature")

resolvers += "Elegant Coding Releases" at "http://elegantcoding.github.io/repo/releases"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "com.typesafe" % "config" % "1.0.2",
  "org.neo4j" % "neo4j" % "2.1.2",
  "org.clapper" %% "grizzled-slf4j" % "1.0.2",
  "org.slf4j" % "slf4j-simple" % "1.7.7",
  "com.elegantcoding" %% "rdf-processor" % "0.4.0"
)

fork in Test := true

javaOptions in Test += "-Xmx4g"

seq(lsSettings :_*)

(LsKeys.tags in LsKeys.lsync) := Seq("rdf","neo4j", "neo")

(description in LsKeys.lsync) :=
  "Convert RDF to neo4j."

instrumentSettings

coverallsSettings

CoverallsKeys.coverallsToken := Some("")
