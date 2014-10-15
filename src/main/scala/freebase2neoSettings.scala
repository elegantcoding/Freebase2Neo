package com.elegantcoding.freebase2neo

import com.elegantcoding.rdfprocessor.rdftriple.types.RdfTriple

import collection.JavaConverters._
import com.typesafe.config._

class Settings {
  val config = ConfigFactory.load(
                 "freebase2neo",
                 ConfigParseOptions.defaults()
                   .setSyntax(ConfigSyntax.JSON)
                   .setAllowMissing(false),
                 ConfigResolveOptions.defaults()
                   .setUseSystemEnvironment(false))

  val freebaseRdfPrefix = config.getString("freebaseRdfPrefix")
  val outputGraphPath = config.getString("outputGraphPath")
  val gzippedNTripleFile = config.getString("gzippedNTripleFile")
  val errorLogFile = config.getString("errorLogFile")
  val statusLogFile = config.getString("statusLogFile")
  val nodeTypePredicates = getConfigList("nodeTypePredicates")


  //TODO move all of this filter functionality elsewhere

  def startsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.startsWith(v)) {true})
    false
  }

  def endsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.endsWith(v)) {true})
    false
  }

  def containsAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.contains(v)) {true})
    false
  }

  case class RdfFilterSetting(
    startsWithSeq : Seq[String] = Seq[String](),
    endsWithSeq : Seq[String] = Seq[String](),
    containsSeq : Seq[String] = Seq[String](),
    equalsSeq : Seq[String] = Seq[String]()
  )
  case class RdfFilter(
    whitelist : RdfFilterSetting = RdfFilterSetting(),
    blacklist : RdfFilterSetting = RdfFilterSetting()
  )

  case class Filters(subjectFilter : RdfFilter, predicateFilter : RdfFilter, objectFilter : RdfFilter) {

    def matchFilter(string : String, filter : RdfFilter) = {

        filter.blacklist.equalsSeq.contains(string) &&
        startsWithAny(string, filter.blacklist.containsSeq) &&
        endsWithAny(string, filter.blacklist.endsWithSeq) &&
        containsAny(string, filter.blacklist.startsWithSeq) &&
        !filter.whitelist.equalsSeq.contains(string) &&
        !startsWithAny(string, filter.whitelist.containsSeq) &&
        !endsWithAny(string, filter.whitelist.endsWithSeq) &&
        !containsAny(string, filter.whitelist.startsWithSeq)
    }

    def matchSubject(string : String) = {

      matchFilter(string, subjectFilter)
    }

    def matchPredicate(string : String) = {

      matchFilter(string, predicateFilter)
    }

    def matchObject(string : String) = {

      matchFilter(string, objectFilter)
    }

    def matchRdf(rdfTriple : RdfTriple) = {

        matchSubject(rdfTriple.subjectString) &&
        matchPredicate(rdfTriple.predicateString) &&
        matchObject(rdfTriple.objectString)
    }
  }


  def getConfigList(configString : String)  = {
    try {
      config.getList(configString).unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
    } catch {
      case ex : Exception => Seq[String]()
    }
  }

  val filters = Filters(
    subjectFilter = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.subject.whitelist.startsWith"),
        endsWithSeq = getConfigList("filters.subject.whitelist.endsWith"),
        containsSeq = getConfigList("filters.subject.whitelist.contains"),
        equalsSeq = getConfigList("filters.subject.whitelist.equals")),
      blacklist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.subject.blacklist.startsWith"),
        endsWithSeq = getConfigList("filters.subject.blacklist.endsWith"),
        containsSeq = getConfigList("filters.subject.blacklist.contains"),
        equalsSeq = getConfigList("filters.subject.blacklist.equals"))
    ),
    predicateFilter = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.predicate.whitelist.startsWith"),
        endsWithSeq = getConfigList("filters.predicate.whitelist.endsWith"),
        containsSeq = getConfigList("filters.predicate.whitelist.contains"),
        equalsSeq = getConfigList("filters.predicate.whitelist.equals")
      ),
      blacklist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.predicate.blacklist.startsWith"),
        endsWithSeq = getConfigList("filters.predicate.blacklist.endsWith"),
        containsSeq = getConfigList("filters.predicate.blacklist.contains"),
        equalsSeq = getConfigList("filters.predicate.blacklist.equals")
      )
    ),
    objectFilter = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.object.whitelist.startsWith"),
        endsWithSeq = getConfigList("filters.object.whitelist.endsWith"),
        containsSeq = getConfigList("filters.object.whitelist.contains"),
        equalsSeq = getConfigList("filters.object.whitelist.equals")
      ),
      blacklist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.object.blacklist.startsWith"),
        endsWithSeq = getConfigList("filters.object.blacklist.endsWith"),
        containsSeq = getConfigList("filters.object.blacklist.contains"),
        equalsSeq = getConfigList("filters.object.blacklist.equals")
      )
    )
  )
}
