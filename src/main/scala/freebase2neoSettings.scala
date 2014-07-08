package com.elegantcoding.freebase2neo

import collection.JavaConverters._
import com.typesafe.config._

import java.util.ArrayList

object Settings {
  val config = ConfigFactory.load(
                 "rdf2neo",
                 ConfigParseOptions.defaults()
                   .setSyntax(ConfigSyntax.JSON)
                   .setAllowMissing(false),
                 ConfigResolveOptions.defaults()
                   .setUseSystemEnvironment(false))

  val fbRdfPrefix = config.getString("fbRdfPrefix")
  val fbRdfPrefixLen = fbRdfPrefix.length()

  val outputGraphPath = config.getString("outputGraphPath")
  val gzippedNTripleFile = config.getString("gzippedNTripleFile")
  val errorLogFile = config.getString("errorLogFile")
  val statusLogFile = config.getString("statusLogFile")
  val nodeTypeSubjects = config.getList("nodeTypeSubjects").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val ignorePredicates = config.getList("ignorePredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val nodeTypeSubjectsConjunctive = config.getList("nodeTypeSubjectsConjunctive").unwrapped.asScala.toSeq.map(_.asInstanceOf[ArrayList[String]].asScala.toSeq)

  val nodeTypePredicates = config.getList("nodeTypePredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
}
