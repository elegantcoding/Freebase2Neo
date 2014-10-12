package com.elegantcoding.freebase2neo

import collection.JavaConverters._
import com.typesafe.config._

import java.util.ArrayList

object Settings {
  val config = ConfigFactory.load(
                 "freebase2neo",
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
  val allowPredicates = config.getList("allowPredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val allowObjectPrefix = config.getList("allowObjectPrefix").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val allowObjectSuffix = config.getList("allowObjectSuffix").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val ignoreSubjectPrefixes = config.getList("ignoreSubjectPrefixes").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val ignorePredicates = config.getList("ignorePredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val ignorePredicatePrefixes = config.getList("ignorePredicatePrefixes").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val ignoreObjectPrefixes = config.getList("ignoreObjectPrefixes").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val ignoreObjectContains = config.getList("ignoreObjectContains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val nodeTypeSubjectsConjunctive = config.getList("nodeTypeSubjectsConjunctive").unwrapped.asScala.toSeq.map(_.asInstanceOf[ArrayList[String]].asScala.toSeq)
  val nodeTypePredicates = config.getList("nodeTypePredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
}
