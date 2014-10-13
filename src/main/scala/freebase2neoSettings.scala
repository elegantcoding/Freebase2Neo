package com.elegantcoding.freebase2neo

import com.elegantcoding.rdfprocessor.rdftriple.types.RdfTriple

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

  val freebaseRdfPrefix = config.getString("freebaseRdfPrefix")
  val outputGraphPath = config.getString("outputGraphPath")
  val gzippedNTripleFile = config.getString("gzippedNTripleFile")
  val errorLogFile = config.getString("errorLogFile")
  val statusLogFile = config.getString("statusLogFile")
  val nodeTypePredicates = getConfigList("nodeTypePredicates")

  def startsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.startsWith(v)) {return true})
    return false
  }

  def endsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.endsWith(v)) {return true})
    return false
  }

  def containsAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.contains(v)) {return true})
    return false
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

  case class Filters(subject : RdfFilter, predicate : RdfFilter, obj:RdfFilter) {

      def matchRdf(rdfTriple : RdfTriple) : Boolean = {
        true
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
    subject = RdfFilter(
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
    predicate = RdfFilter(
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
    obj = RdfFilter(
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
